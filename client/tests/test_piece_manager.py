"""
Tests for pieceManager.py — piece tracking, block assembly, SHA-1 verification,
rarest-first selection, peer bitfield tracking, and progress reporting.
"""

import hashlib
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.peerProtocol import BLOCK_SIZE
from modules.pieceManager import Piece, PieceManager, PieceState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_piece_data(length: int, fill: int = 0xAB) -> bytes:
    """Create deterministic piece data of *length* bytes."""
    return bytes([fill % 256]) * length


def _sha1(data: bytes) -> bytes:
    return hashlib.sha1(data).digest()


def _make_piece(length: int = BLOCK_SIZE * 2, fill: int = 0xAB) -> tuple[Piece, bytes]:
    """Return (Piece, raw_data) with a correct SHA-1 hash."""
    data = _make_piece_data(length, fill)
    piece = Piece(index=0, length=length, expected_hash=_sha1(data))
    return piece, data


def _fill_piece(piece: Piece, data: bytes) -> None:
    """Add all blocks of *data* to *piece* in 16 KB increments."""
    offset = 0
    while offset < len(data):
        block = data[offset : offset + BLOCK_SIZE]
        piece.add_block(offset, block)
        offset += len(block)


def _make_manager(
    num_pieces: int = 4,
    piece_length: int = BLOCK_SIZE * 2,
    last_piece_length: int | None = None,
    fill: int = 0xAB,
) -> tuple[PieceManager, list[bytes]]:
    """
    Return (PieceManager, list_of_piece_data).
    All pieces have correct SHA-1 hashes baked in.
    """
    if last_piece_length is None:
        last_piece_length = piece_length

    piece_data = [_make_piece_data(piece_length, fill + i) for i in range(num_pieces - 1)]
    piece_data.append(_make_piece_data(last_piece_length, fill + num_pieces - 1))

    hashes      = [_sha1(d) for d in piece_data]
    total_length = piece_length * (num_pieces - 1) + last_piece_length

    mgr = PieceManager(
        piece_hashes=hashes,
        piece_length=piece_length,
        total_length=total_length,
    )
    return mgr, piece_data


def _add_all_blocks(mgr: PieceManager, piece_index: int, data: bytes) -> bool:
    """Feed all blocks for one piece into the manager. Returns final add_block result."""
    result = False
    offset = 0
    while offset < len(data):
        block  = data[offset : offset + BLOCK_SIZE]
        result = mgr.add_block(piece_index, offset, block)
        offset += len(block)
    return result


PEER_A = b"peer_A" + b"\x00" * 14
PEER_B = b"peer_B" + b"\x00" * 14


# ---------------------------------------------------------------------------
# Piece — construction
# ---------------------------------------------------------------------------

class TestPieceConstruction(unittest.TestCase):

    def test_basic_attributes(self):
        data   = _make_piece_data(BLOCK_SIZE)
        piece  = Piece(index=3, length=BLOCK_SIZE, expected_hash=_sha1(data))
        self.assertEqual(piece.index, 3)
        self.assertEqual(piece.length, BLOCK_SIZE)
        self.assertEqual(piece.state, PieceState.NEEDED)

    def test_num_blocks_exact(self):
        piece, _ = _make_piece(BLOCK_SIZE * 4)
        self.assertEqual(piece.num_blocks, 4)

    def test_num_blocks_partial(self):
        # 16 KB + 1 byte → 2 blocks
        piece, _ = _make_piece(BLOCK_SIZE + 1)
        self.assertEqual(piece.num_blocks, 2)

    def test_invalid_hash_length_raises(self):
        with self.assertRaises(ValueError):
            Piece(index=0, length=BLOCK_SIZE, expected_hash=b"\xab" * 10)

    def test_invalid_length_raises(self):
        with self.assertRaises(ValueError):
            Piece(index=0, length=0, expected_hash=b"\xab" * 20)


# ---------------------------------------------------------------------------
# Piece — add_block
# ---------------------------------------------------------------------------

class TestPieceAddBlock(unittest.TestCase):

    def test_add_single_block(self):
        piece, data = _make_piece(BLOCK_SIZE)
        piece.add_block(0, data)
        self.assertTrue(piece.is_complete_raw)

    def test_add_multiple_blocks(self):
        piece, data = _make_piece(BLOCK_SIZE * 3)
        piece.add_block(0,             data[:BLOCK_SIZE])
        piece.add_block(BLOCK_SIZE,    data[BLOCK_SIZE:BLOCK_SIZE*2])
        piece.add_block(BLOCK_SIZE*2,  data[BLOCK_SIZE*2:])
        self.assertTrue(piece.is_complete_raw)

    def test_not_complete_until_all_blocks(self):
        piece, data = _make_piece(BLOCK_SIZE * 2)
        piece.add_block(0, data[:BLOCK_SIZE])
        self.assertFalse(piece.is_complete_raw)

    def test_offset_out_of_range_raises(self):
        piece, data = _make_piece(BLOCK_SIZE)
        with self.assertRaises(ValueError):
            piece.add_block(BLOCK_SIZE, b"\x00")    # offset == length

    def test_block_overflow_raises(self):
        piece, data = _make_piece(BLOCK_SIZE)
        with self.assertRaises(ValueError):
            piece.add_block(BLOCK_SIZE - 1, b"\x00" * 10)  # goes past end

    def test_duplicate_block_overwrites(self):
        piece, data = _make_piece(BLOCK_SIZE)
        piece.add_block(0, b"\x00" * BLOCK_SIZE)
        piece.add_block(0, data)                   # correct data overwrites
        self.assertTrue(piece.verify())


# ---------------------------------------------------------------------------
# Piece — missing_blocks
# ---------------------------------------------------------------------------

class TestPieceMissingBlocks(unittest.TestCase):

    def test_all_missing_initially(self):
        piece, data = _make_piece(BLOCK_SIZE * 3)
        missing = piece.missing_blocks
        self.assertEqual(len(missing), 3)
        self.assertEqual(missing[0], (0, BLOCK_SIZE))

    def test_no_missing_after_all_added(self):
        piece, data = _make_piece(BLOCK_SIZE * 2)
        _fill_piece(piece, data)
        self.assertEqual(piece.missing_blocks, [])

    def test_partial_missing(self):
        piece, data = _make_piece(BLOCK_SIZE * 3)
        piece.add_block(0, data[:BLOCK_SIZE])
        missing = piece.missing_blocks
        self.assertEqual(len(missing), 2)
        offsets = [m[0] for m in missing]
        self.assertNotIn(0, offsets)

    def test_last_block_correct_length(self):
        length = BLOCK_SIZE + 1000     # not a multiple of BLOCK_SIZE
        data   = _make_piece_data(length)
        piece  = Piece(index=0, length=length, expected_hash=_sha1(data))
        missing = piece.missing_blocks
        # Last block should be 1000 bytes, not BLOCK_SIZE
        self.assertEqual(missing[-1][1], 1000)


# ---------------------------------------------------------------------------
# Piece — verify
# ---------------------------------------------------------------------------

class TestPieceVerify(unittest.TestCase):

    def test_correct_hash_returns_true(self):
        piece, data = _make_piece(BLOCK_SIZE * 2)
        _fill_piece(piece, data)
        self.assertTrue(piece.verify())
        self.assertEqual(piece.state, PieceState.COMPLETE)

    def test_wrong_hash_returns_false(self):
        data  = _make_piece_data(BLOCK_SIZE)
        piece = Piece(index=0, length=BLOCK_SIZE, expected_hash=b"\x00" * 20)
        _fill_piece(piece, data)
        self.assertFalse(piece.verify())
        # After a hash mismatch, verify() immediately resets the piece to NEEDED
        # (FAILED is a transient internal state — the piece is ready to re-request)
        self.assertEqual(piece.state, PieceState.NEEDED)


    def test_failed_piece_resets_to_needed(self):
        data  = _make_piece_data(BLOCK_SIZE)
        piece = Piece(index=0, length=BLOCK_SIZE, expected_hash=b"\x00" * 20)
        _fill_piece(piece, data)
        piece.verify()   # fails
        self.assertEqual(piece.state, PieceState.NEEDED)
        self.assertFalse(piece.is_complete_raw)

    def test_incomplete_piece_verify_returns_false(self):
        piece, data = _make_piece(BLOCK_SIZE * 2)
        piece.add_block(0, data[:BLOCK_SIZE])   # only first block
        self.assertFalse(piece.verify())

    def test_data_property_after_complete(self):
        piece, data = _make_piece(BLOCK_SIZE * 2)
        _fill_piece(piece, data)
        piece.verify()
        self.assertEqual(piece.data, data)

    def test_data_property_raises_if_not_complete(self):
        piece, data = _make_piece(BLOCK_SIZE)
        with self.assertRaises(RuntimeError):
            _ = piece.data


# ---------------------------------------------------------------------------
# PieceManager — construction
# ---------------------------------------------------------------------------

class TestPieceManagerConstruction(unittest.TestCase):

    def test_num_pieces(self):
        mgr, _ = _make_manager(num_pieces=5)
        self.assertEqual(mgr._num_pieces, 5)

    def test_initial_progress_is_zero(self):
        mgr, _ = _make_manager(num_pieces=4)
        self.assertEqual(mgr.get_progress(), 0.0)

    def test_last_piece_shorter(self):
        piece_length      = BLOCK_SIZE * 2
        last_piece_length = 1234
        mgr, _            = _make_manager(
            num_pieces=3,
            piece_length=piece_length,
            last_piece_length=last_piece_length,
        )
        self.assertEqual(mgr._pieces[-1].length, last_piece_length)
        self.assertEqual(mgr._pieces[0].length, piece_length)

    def test_repr(self):
        mgr, _ = _make_manager(num_pieces=4)
        r = repr(mgr)
        self.assertIn("0/4", r)


# ---------------------------------------------------------------------------
# PieceManager — peer tracking
# ---------------------------------------------------------------------------

class TestPieceManagerPeerTracking(unittest.TestCase):

    def test_on_peer_bitfield_all_ones(self):
        mgr, _ = _make_manager(num_pieces=4)
        # 4 pieces → 1 byte bitfield; 0xF0 = first 4 bits set
        mgr.on_peer_bitfield(PEER_A, b"\xf0")
        for i in range(4):
            self.assertTrue(mgr.peer_has_piece(PEER_A, i))

    def test_on_peer_bitfield_partial(self):
        mgr, _ = _make_manager(num_pieces=8)
        # 0b10000000 = 0x80 → only piece 0
        mgr.on_peer_bitfield(PEER_A, b"\x80")
        self.assertTrue(mgr.peer_has_piece(PEER_A, 0))
        self.assertFalse(mgr.peer_has_piece(PEER_A, 1))

    def test_on_peer_have(self):
        mgr, _ = _make_manager(num_pieces=4)
        mgr.on_peer_have(PEER_A, 2)
        self.assertTrue(mgr.peer_has_piece(PEER_A, 2))
        self.assertFalse(mgr.peer_has_piece(PEER_A, 0))

    def test_remove_peer(self):
        mgr, _ = _make_manager(num_pieces=4)
        mgr.on_peer_have(PEER_A, 0)
        mgr.remove_peer(PEER_A)
        self.assertFalse(mgr.peer_has_piece(PEER_A, 0))

    def test_unknown_peer_has_no_pieces(self):
        mgr, _ = _make_manager(num_pieces=4)
        self.assertFalse(mgr.peer_has_piece(b"unknown" + b"\x00" * 13, 0))


# ---------------------------------------------------------------------------
# PieceManager — select_piece (rarest-first)
# ---------------------------------------------------------------------------

class TestPieceManagerSelectPiece(unittest.TestCase):

    def test_returns_none_when_peer_has_nothing(self):
        mgr, _ = _make_manager(num_pieces=4)
        # peer not registered → no pieces
        result = mgr.select_piece(PEER_A)
        self.assertIsNone(result)

    def test_returns_piece_peer_has(self):
        mgr, _ = _make_manager(num_pieces=4)
        mgr.on_peer_have(PEER_A, 2)
        result = mgr.select_piece(PEER_A)
        self.assertEqual(result, 2)

    def test_piece_marked_pending_after_selection(self):
        mgr, _ = _make_manager(num_pieces=4)
        mgr.on_peer_have(PEER_A, 1)
        idx = mgr.select_piece(PEER_A)
        self.assertEqual(mgr._pieces[idx].state, PieceState.PENDING)
        self.assertIn(idx, mgr._pending)

    def test_pending_piece_not_selected_again(self):
        mgr, _ = _make_manager(num_pieces=4)
        mgr.on_peer_have(PEER_A, 0)
        idx1 = mgr.select_piece(PEER_A)
        idx2 = mgr.select_piece(PEER_A)   # no more pieces for this peer
        self.assertEqual(idx1, 0)
        self.assertIsNone(idx2)

    def test_rarest_first_prefers_rarer_piece(self):
        mgr, _ = _make_manager(num_pieces=4)
        # PEER_A has pieces 0 and 1
        # PEER_B also has piece 0 but not piece 1
        # So piece 1 is rarer — should be selected first
        mgr.on_peer_have(PEER_A, 0)
        mgr.on_peer_have(PEER_A, 1)
        mgr.on_peer_have(PEER_B, 0)
        result = mgr.select_piece(PEER_A)
        self.assertEqual(result, 1)   # rarer (only PEER_A has it)

    def test_completed_piece_not_selected(self):
        mgr, pieces = _make_manager(num_pieces=2)
        # Complete piece 0
        _add_all_blocks(mgr, 0, pieces[0])
        mgr.on_peer_have(PEER_A, 0)
        mgr.on_peer_have(PEER_A, 1)
        result = mgr.select_piece(PEER_A)
        self.assertEqual(result, 1)


# ---------------------------------------------------------------------------
# PieceManager — add_block / verification
# ---------------------------------------------------------------------------

class TestPieceManagerAddBlock(unittest.TestCase):

    def test_incomplete_piece_returns_false(self):
        mgr, pieces = _make_manager(num_pieces=2)
        mgr.on_peer_have(PEER_A, 0)
        mgr.select_piece(PEER_A)
        result = mgr.add_block(0, 0, pieces[0][:BLOCK_SIZE])
        self.assertFalse(result)

    def test_complete_piece_returns_true(self):
        mgr, pieces = _make_manager(num_pieces=2)
        result = _add_all_blocks(mgr, 0, pieces[0])
        self.assertTrue(result)

    def test_complete_piece_state_is_complete(self):
        mgr, pieces = _make_manager(num_pieces=2)
        _add_all_blocks(mgr, 0, pieces[0])
        self.assertEqual(mgr._pieces[0].state, PieceState.COMPLETE)

    def test_progress_updates_after_piece_complete(self):
        mgr, pieces = _make_manager(num_pieces=4)
        _add_all_blocks(mgr, 0, pieces[0])
        self.assertAlmostEqual(mgr.get_progress(), 0.25)

    def test_bad_hash_resets_piece(self):
        num_pieces   = 2
        piece_length = BLOCK_SIZE * 2
        # Use wrong hashes so verification always fails
        bad_hash   = b"\x00" * 20
        mgr        = PieceManager(
            piece_hashes=[bad_hash, bad_hash],
            piece_length=piece_length,
            total_length=piece_length * num_pieces,
        )
        data   = _make_piece_data(piece_length)
        result = _add_all_blocks(mgr, 0, data)
        self.assertFalse(result)
        self.assertEqual(mgr._pieces[0].state, PieceState.NEEDED)

    def test_failed_piece_removed_from_pending(self):
        piece_length = BLOCK_SIZE * 2
        bad_hash     = b"\x00" * 20
        mgr          = PieceManager(
            piece_hashes=[bad_hash],
            piece_length=piece_length,
            total_length=piece_length,
        )
        mgr._pending.add(0)
        _add_all_blocks(mgr, 0, _make_piece_data(piece_length))
        self.assertNotIn(0, mgr._pending)

    def test_all_pieces_complete_is_done(self):
        mgr, pieces = _make_manager(num_pieces=3)
        for i, data in enumerate(pieces):
            _add_all_blocks(mgr, i, data)
        self.assertTrue(mgr.is_done)
        self.assertEqual(mgr.get_progress(), 1.0)


# ---------------------------------------------------------------------------
# PieceManager — next_block_request
# ---------------------------------------------------------------------------

class TestNextBlockRequest(unittest.TestCase):

    def test_returns_first_missing_block(self):
        mgr, _ = _make_manager(num_pieces=2)
        req = mgr.next_block_request(0)
        self.assertIsNotNone(req)
        piece_idx, offset, length = req
        self.assertEqual(piece_idx, 0)
        self.assertEqual(offset, 0)
        self.assertLessEqual(length, BLOCK_SIZE)

    def test_returns_none_when_all_received(self):
        mgr, pieces = _make_manager(num_pieces=2)
        _add_all_blocks(mgr, 0, pieces[0])
        req = mgr.next_block_request(0)
        self.assertIsNone(req)


# ---------------------------------------------------------------------------
# PieceManager — mark_piece_failed
# ---------------------------------------------------------------------------

class TestMarkPieceFailed(unittest.TestCase):

    def test_resets_to_needed(self):
        mgr, pieces = _make_manager(num_pieces=2)
        mgr.on_peer_have(PEER_A, 0)
        mgr.select_piece(PEER_A)
        mgr.add_block(0, 0, pieces[0][:BLOCK_SIZE])
        mgr.mark_piece_failed(0)
        self.assertEqual(mgr._pieces[0].state, PieceState.NEEDED)
        self.assertNotIn(0, mgr._pending)

    def test_failed_piece_can_be_selected_again(self):
        mgr, _ = _make_manager(num_pieces=2)
        mgr.on_peer_have(PEER_A, 0)
        mgr.select_piece(PEER_A)
        mgr.mark_piece_failed(0)
        result = mgr.select_piece(PEER_A)
        self.assertEqual(result, 0)


# ---------------------------------------------------------------------------
# PieceManager — get_bitfield
# ---------------------------------------------------------------------------

class TestGetBitfield(unittest.TestCase):

    def test_empty_bitfield_initially(self):
        mgr, _ = _make_manager(num_pieces=8)
        self.assertEqual(mgr.get_bitfield(), b"\x00")

    def test_bitfield_after_completing_piece_0(self):
        mgr, pieces = _make_manager(num_pieces=8)
        _add_all_blocks(mgr, 0, pieces[0])
        bf = mgr.get_bitfield()
        # Bit 7 of byte 0 = piece 0 → 0b10000000 = 0x80
        self.assertEqual(bf[0] & 0x80, 0x80)

    def test_bitfield_all_complete(self):
        mgr, pieces = _make_manager(num_pieces=8)
        for i, data in enumerate(pieces):
            _add_all_blocks(mgr, i, data)
        self.assertEqual(mgr.get_bitfield(), b"\xff")

    def test_get_piece_data(self):
        mgr, pieces = _make_manager(num_pieces=2)
        _add_all_blocks(mgr, 0, pieces[0])
        self.assertEqual(mgr.get_piece_data(0), pieces[0])


if __name__ == "__main__":
    unittest.main()
