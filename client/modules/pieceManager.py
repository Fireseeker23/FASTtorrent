"""
pieceManager.py — Piece tracking and selection for the BitTorrent protocol.

Responsibilities:
  - Track which pieces are needed, pending (in-flight), and complete
  - Assemble 16 KB blocks into full pieces
  - Verify each completed piece against its SHA-1 hash (from the .torrent)
  - Track what each peer has (via Bitfield and Have messages)
  - Select which piece to download next using rarest-first strategy
"""

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Tuple

from modules.peerProtocol import BLOCK_SIZE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Piece state
# ---------------------------------------------------------------------------

class PieceState(Enum):
    NEEDED   = auto()   # not started
    PENDING  = auto()   # requested from a peer, waiting for all blocks
    COMPLETE = auto()   # all blocks received AND SHA-1 verified
    FAILED   = auto()   # SHA-1 mismatch — will be reset to NEEDED


# ---------------------------------------------------------------------------
# Piece
# ---------------------------------------------------------------------------

class Piece:
    """
    Represents one torrent piece.

    A piece is divided into fixed-size blocks (16 KB by default). Blocks
    arrive out of order and are stored by byte offset. Once all blocks
    are present the piece is verified against its expected SHA-1 hash.
    """

    def __init__(self, index: int, length: int, expected_hash: bytes) -> None:
        if len(expected_hash) != 20:
            raise ValueError("expected_hash must be 20 bytes (SHA-1)")
        if length <= 0:
            raise ValueError(f"Piece length must be positive, got {length}")

        self.index:         int        = index
        self.length:        int        = length
        self.expected_hash: bytes      = expected_hash
        self.state:         PieceState = PieceState.NEEDED

        # Sparse storage: offset → data bytes
        self._blocks: Dict[int, bytes] = {}
        self._bytes_received: int      = 0

    # ------------------------------------------------------------------
    # Block management
    # ------------------------------------------------------------------

    def add_block(self, offset: int, data: bytes) -> None:
        """
        Store a received block.

        Raises:
            ValueError: if the offset or data length would overflow the piece.
        """
        if offset < 0 or offset >= self.length:
            raise ValueError(
                f"Piece {self.index}: block offset {offset} out of range "
                f"[0, {self.length})"
            )
        if offset + len(data) > self.length:
            raise ValueError(
                f"Piece {self.index}: block at offset {offset} with length "
                f"{len(data)} exceeds piece length {self.length}"
            )
        if offset not in self._blocks:
            self._bytes_received += len(data)
        self._blocks[offset] = data

    @property
    def is_complete_raw(self) -> bool:
        """True when all expected bytes have been received (before SHA-1 check)."""
        return self._bytes_received >= self.length

    @property
    def missing_blocks(self) -> List[Tuple[int, int]]:
        """
        Return a list of (offset, length) for blocks not yet received.
        Used by the PieceManager to know what to request next.
        """
        missing = []
        offset  = 0
        while offset < self.length:
            size = min(BLOCK_SIZE, self.length - offset)
            if offset not in self._blocks:
                missing.append((offset, size))
            offset += size
        return missing

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify(self) -> bool:
        """
        Reassemble the piece and compare its SHA-1 to the expected hash.

        Returns True on success and marks state COMPLETE.
        Returns False on mismatch and marks state FAILED (so it can be
        re-requested).
        """
        if not self.is_complete_raw:
            return False

        data = self._assemble()
        actual_hash = hashlib.sha1(data).digest()

        if actual_hash == self.expected_hash:
            self.state = PieceState.COMPLETE
            logger.debug("Piece %d verified OK", self.index)
            return True
        else:
            logger.warning(
                "Piece %d hash mismatch! expected=%s got=%s",
                self.index,
                self.expected_hash.hex(),
                actual_hash.hex(),
            )
            self.state = PieceState.FAILED
            self._reset()
            return False

    def _assemble(self) -> bytes:
        """Concatenate blocks in offset order into a single bytes object."""
        return b"".join(self._blocks[o] for o in sorted(self._blocks))

    def _reset(self) -> None:
        """Clear all received blocks so the piece can be re-requested."""
        self._blocks.clear()
        self._bytes_received = 0
        self.state = PieceState.NEEDED

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def data(self) -> bytes:
        """Return the assembled piece bytes. Only valid after verify() → True."""
        if self.state != PieceState.COMPLETE:
            raise RuntimeError(
                f"Piece {self.index} is not complete (state={self.state.name})"
            )
        return self._assemble()

    @property
    def num_blocks(self) -> int:
        """Total number of 16 KB blocks this piece is divided into."""
        return (self.length + BLOCK_SIZE - 1) // BLOCK_SIZE

    def __repr__(self) -> str:
        pct = 100 * self._bytes_received // self.length
        return f"Piece(index={self.index}, state={self.state.name}, {pct}%)"


# ---------------------------------------------------------------------------
# PieceManager
# ---------------------------------------------------------------------------

class PieceManager:
    """
    Tracks all pieces across the entire torrent.

    Responsibilities:
      - Maintain state (NEEDED / PENDING / COMPLETE) for every piece
      - Track each connected peer's availability bitfield
      - Select which piece to download next (rarest-first)
      - Accept incoming blocks and trigger verification on completion
      - Report overall progress
    """

    def __init__(
        self,
        piece_hashes:  List[bytes],
        piece_length:  int,
        total_length:  int,
    ) -> None:
        """
        Args:
            piece_hashes:  List of 20-byte SHA-1 hashes, one per piece.
            piece_length:  Size of each piece in bytes (last piece may be smaller).
            total_length:  Total bytes across all files.
        """
        self._piece_length = piece_length
        self._total_length = total_length
        self._num_pieces   = len(piece_hashes)

        # Build Piece objects
        self._pieces: List[Piece] = []
        for i, h in enumerate(piece_hashes):
            # Last piece may be shorter
            if i < self._num_pieces - 1:
                length = piece_length
            else:
                remainder = total_length % piece_length
                length    = remainder if remainder > 0 else piece_length
            self._pieces.append(Piece(index=i, length=length, expected_hash=h))

        # peer_id → set of piece indices the peer has
        self._peer_pieces: Dict[bytes, Set[int]] = {}

        # Pieces currently being requested (to avoid double-requesting)
        self._pending: Set[int] = set()

        logger.info(
            "PieceManager initialised: %d pieces, %d KB each, total %.2f MB",
            self._num_pieces,
            piece_length // 1024,
            total_length / 1024 / 1024,
        )

    # ------------------------------------------------------------------
    # Peer tracking
    # ------------------------------------------------------------------

    def on_peer_bitfield(self, peer_id: bytes, bitfield: bytes) -> None:
        """
        Record what a peer has from their Bitfield message (sent right
        after the handshake).
        """
        available: Set[int] = set()
        for byte_i, byte_val in enumerate(bitfield):
            for bit_i in range(8):
                piece_index = byte_i * 8 + (7 - bit_i)
                if piece_index < self._num_pieces and (byte_val >> bit_i) & 1:
                    available.add(piece_index)
        self._peer_pieces[peer_id] = available
        logger.debug("Peer %s has %d/%d pieces", peer_id, len(available), self._num_pieces)

    def on_peer_have(self, peer_id: bytes, piece_index: int) -> None:
        """Record that a peer just completed a piece (Have message)."""
        if peer_id not in self._peer_pieces:
            self._peer_pieces[peer_id] = set()
        self._peer_pieces[peer_id].add(piece_index)

    def remove_peer(self, peer_id: bytes) -> None:
        """Called when a peer disconnects — remove their availability record."""
        self._peer_pieces.pop(peer_id, None)

    def peer_has_piece(self, peer_id: bytes, piece_index: int) -> bool:
        """Return True if the given peer has the given piece."""
        return piece_index in self._peer_pieces.get(peer_id, set())

    # ------------------------------------------------------------------
    # Piece selection — rarest-first
    # ------------------------------------------------------------------

    def select_piece(self, peer_id: bytes) -> Optional[int]:
        """
        Choose the next piece to request from *peer_id*.

        Strategy: **rarest-first** — pick the NEEDED piece that the fewest
        peers have, prioritising pieces that *this* peer can serve.

        Returns:
            Piece index, or None if no suitable piece exists.
        """
        peer_available = self._peer_pieces.get(peer_id, set())

        # Candidates: NEEDED pieces that this peer has and aren't already pending
        candidates = [
            i for i in peer_available
            if (
                self._pieces[i].state == PieceState.NEEDED
                and i not in self._pending
            )
        ]

        if not candidates:
            return None

        # Count how many peers have each candidate piece
        def _rarity(piece_index: int) -> int:
            return sum(
                1 for ps in self._peer_pieces.values()
                if piece_index in ps
            )

        rarest_index = min(candidates, key=_rarity)
        self._pieces[rarest_index].state = PieceState.PENDING
        self._pending.add(rarest_index)
        return rarest_index

    def next_block_request(
        self, piece_index: int
    ) -> Optional[Tuple[int, int, int]]:
        """
        Return the next (piece_index, offset, length) block to request for
        the given piece, or None if all blocks have been received.
        """
        piece   = self._pieces[piece_index]
        missing = piece.missing_blocks
        if not missing:
            return None
        offset, length = missing[0]
        return (piece_index, offset, length)

    # ------------------------------------------------------------------
    # Receiving data
    # ------------------------------------------------------------------

    def add_block(
        self, piece_index: int, offset: int, data: bytes
    ) -> bool:
        """
        Store a received block.

        Returns:
            True  — the piece is now complete AND verified (SHA-1 matched).
            False — more blocks still needed, or SHA-1 failed (piece reset).

        Raises:
            IndexError:  if piece_index is out of range.
            ValueError:  if the block overflows the piece.
        """
        piece = self._pieces[piece_index]
        piece.add_block(offset, data)

        if piece.is_complete_raw:
            ok = piece.verify()
            self._pending.discard(piece_index)
            if not ok:
                logger.warning("Piece %d failed verification, will re-request", piece_index)
            return ok

        return False

    def mark_piece_failed(self, piece_index: int) -> None:
        """
        Explicitly mark a piece as failed (e.g. peer disconnected mid-transfer).
        Resets it to NEEDED so it can be re-requested.
        """
        piece = self._pieces[piece_index]
        piece._reset()
        self._pending.discard(piece_index)

    # ------------------------------------------------------------------
    # Progress
    # ------------------------------------------------------------------

    @property
    def num_complete(self) -> int:
        return sum(1 for p in self._pieces if p.state == PieceState.COMPLETE)

    @property
    def num_needed(self) -> int:
        return sum(1 for p in self._pieces if p.state == PieceState.NEEDED)

    @property
    def is_done(self) -> bool:
        return self.num_complete == self._num_pieces

    def get_progress(self) -> float:
        """Return download progress as a float in [0.0, 1.0]."""
        if self._num_pieces == 0:
            return 1.0
        return self.num_complete / self._num_pieces

    def get_bitfield(self) -> bytes:
        """
        Return our own bitfield as bytes (to send to peers after handshake).
        Bit 7 of byte 0 = piece 0, etc.
        """
        n_bytes = (self._num_pieces + 7) // 8
        bf = bytearray(n_bytes)
        for i, piece in enumerate(self._pieces):
            if piece.state == PieceState.COMPLETE:
                byte_i = i // 8
                bit_i  = 7 - (i % 8)
                bf[byte_i] |= (1 << bit_i)
        return bytes(bf)

    def get_piece_data(self, piece_index: int) -> bytes:
        """Return the verified data for a complete piece."""
        return self._pieces[piece_index].data

    def __repr__(self) -> str:
        return (
            f"PieceManager({self.num_complete}/{self._num_pieces} pieces, "
            f"{self.get_progress():.1%})"
        )