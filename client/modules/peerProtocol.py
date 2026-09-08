"""
peerProtocol.py — BitTorrent peer wire protocol (BEP-3).

Implements:
  - Handshake (68-byte fixed format)
  - Message framing: 4-byte big-endian length prefix + 1-byte message ID + payload
  - All standard message types: choke, unchoke, interested, not-interested,
    have, bitfield, request, piece, cancel, keep-alive
  - Full async TCP connection lifecycle via asyncio streams
"""

import asyncio
import struct
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROTOCOL_NAME    = b"BitTorrent protocol"
HANDSHAKE_LEN    = 68          # 1 + 19 + 8 + 20 + 20
BLOCK_SIZE       = 16 * 1024   # 16 KB — standard block request size
KEEP_ALIVE_BYTES = b"\x00\x00\x00\x00"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PeerError(Exception):
    """Raised on a protocol violation or unexpected disconnect."""
    pass


class HandshakeError(PeerError):
    """Raised when the peer handshake is invalid."""
    pass


# ---------------------------------------------------------------------------
# Message IDs
# ---------------------------------------------------------------------------

class MessageID(IntEnum):
    CHOKE          = 0
    UNCHOKE        = 1
    INTERESTED     = 2
    NOT_INTERESTED = 3
    HAVE           = 4
    BITFIELD       = 5
    REQUEST        = 6
    PIECE          = 7
    CANCEL         = 8


# ---------------------------------------------------------------------------
# Message classes
# ---------------------------------------------------------------------------

class Message(ABC):
    """Abstract base for all peer wire protocol messages."""

    @abstractmethod
    def encode(self) -> bytes:
        """Encode the message to bytes ready to be sent over the wire."""
        ...

    @staticmethod
    def _frame(msg_id: int, payload: bytes = b"") -> bytes:
        """Wrap payload in the standard 4-byte length prefix + 1-byte ID frame."""
        body = bytes([msg_id]) + payload
        return struct.pack("!I", len(body)) + body


# ── Fixed-length, no-payload messages ──────────────────────────────────────

class KeepAliveMessage(Message):
    """4 zero bytes — no length prefix, no ID."""
    def encode(self) -> bytes:
        return KEEP_ALIVE_BYTES

    def __repr__(self) -> str:
        return "KeepAlive"


class ChokeMessage(Message):
    def encode(self) -> bytes:
        return self._frame(MessageID.CHOKE)

    def __repr__(self) -> str:
        return "Choke"


class UnchokeMessage(Message):
    def encode(self) -> bytes:
        return self._frame(MessageID.UNCHOKE)

    def __repr__(self) -> str:
        return "Unchoke"


class InterestedMessage(Message):
    def encode(self) -> bytes:
        return self._frame(MessageID.INTERESTED)

    def __repr__(self) -> str:
        return "Interested"


class NotInterestedMessage(Message):
    def encode(self) -> bytes:
        return self._frame(MessageID.NOT_INTERESTED)

    def __repr__(self) -> str:
        return "NotInterested"


# ── Variable-payload messages ───────────────────────────────────────────────

@dataclass
class HaveMessage(Message):
    """Peer announces it has completed downloading a piece."""
    piece_index: int

    def encode(self) -> bytes:
        return self._frame(MessageID.HAVE, struct.pack("!I", self.piece_index))

    def __repr__(self) -> str:
        return f"Have(piece={self.piece_index})"


@dataclass
class BitfieldMessage(Message):
    """
    Sent right after the handshake. Each bit represents whether the peer
    has the corresponding piece (1 = have, 0 = missing).
    Bit 7 of byte 0 = piece 0, bit 6 = piece 1, etc.
    """
    bitfield: bytes

    def encode(self) -> bytes:
        return self._frame(MessageID.BITFIELD, self.bitfield)

    def has_piece(self, index: int) -> bool:
        byte_index = index // 8
        bit_offset = 7 - (index % 8)
        if byte_index >= len(self.bitfield):
            return False
        return bool((self.bitfield[byte_index] >> bit_offset) & 1)

    def __repr__(self) -> str:
        return f"Bitfield(len={len(self.bitfield)})"


@dataclass
class RequestMessage(Message):
    """
    Request a block of data from a peer.

    Fields:
        index:  Piece index (0-based).
        begin:  Byte offset within the piece.
        length: Number of bytes to request (usually BLOCK_SIZE = 16 KB).
    """
    index:  int
    begin:  int
    length: int = BLOCK_SIZE

    def encode(self) -> bytes:
        payload = struct.pack("!III", self.index, self.begin, self.length)
        return self._frame(MessageID.REQUEST, payload)

    def __repr__(self) -> str:
        return f"Request(piece={self.index}, offset={self.begin}, len={self.length})"


@dataclass
class PieceMessage(Message):
    """
    Carries an actual block of downloaded data.

    Fields:
        index:  Piece index.
        begin:  Byte offset within the piece.
        block:  The raw data bytes.
    """
    index: int
    begin: int
    block: bytes

    def encode(self) -> bytes:
        header  = struct.pack("!II", self.index, self.begin)
        return self._frame(MessageID.PIECE, header + self.block)

    def __repr__(self) -> str:
        return f"Piece(piece={self.index}, offset={self.begin}, len={len(self.block)})"


@dataclass
class CancelMessage(Message):
    """Cancel a previously sent Request (used by endgame mode)."""
    index:  int
    begin:  int
    length: int = BLOCK_SIZE

    def encode(self) -> bytes:
        payload = struct.pack("!III", self.index, self.begin, self.length)
        return self._frame(MessageID.CANCEL, payload)

    def __repr__(self) -> str:
        return f"Cancel(piece={self.index}, offset={self.begin})"


# ---------------------------------------------------------------------------
# Message decoder
# ---------------------------------------------------------------------------

def decode_message(msg_id: int, payload: bytes) -> Message:
    """
    Decode a raw (msg_id, payload) pair into a typed Message object.

    Raises:
        PeerError: if the message is malformed or the ID is unknown.
    """
    try:
        mid = MessageID(msg_id)
    except ValueError:
        raise PeerError(f"Unknown message ID: {msg_id}")

    if mid == MessageID.CHOKE:
        return ChokeMessage()
    elif mid == MessageID.UNCHOKE:
        return UnchokeMessage()
    elif mid == MessageID.INTERESTED:
        return InterestedMessage()
    elif mid == MessageID.NOT_INTERESTED:
        return NotInterestedMessage()

    elif mid == MessageID.HAVE:
        if len(payload) != 4:
            raise PeerError(f"Have message payload must be 4 bytes, got {len(payload)}")
        (piece_index,) = struct.unpack("!I", payload)
        return HaveMessage(piece_index=piece_index)

    elif mid == MessageID.BITFIELD:
        return BitfieldMessage(bitfield=payload)

    elif mid == MessageID.REQUEST:
        if len(payload) != 12:
            raise PeerError(f"Request payload must be 12 bytes, got {len(payload)}")
        index, begin, length = struct.unpack("!III", payload)
        return RequestMessage(index=index, begin=begin, length=length)

    elif mid == MessageID.PIECE:
        if len(payload) < 8:
            raise PeerError(f"Piece payload must be >=8 bytes, got {len(payload)}")
        index, begin = struct.unpack_from("!II", payload)
        block = payload[8:]
        return PieceMessage(index=index, begin=begin, block=block)

    elif mid == MessageID.CANCEL:
        if len(payload) != 12:
            raise PeerError(f"Cancel payload must be 12 bytes, got {len(payload)}")
        index, begin, length = struct.unpack("!III", payload)
        return CancelMessage(index=index, begin=begin, length=length)

    raise PeerError(f"Unhandled message ID: {mid}")  # should never reach


# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------

def build_handshake(info_hash: bytes, peer_id: bytes) -> bytes:
    """
    Build the 68-byte BitTorrent handshake:

        1 byte  : length of protocol name (19)
        19 bytes: "BitTorrent protocol"
        8 bytes : reserved (all zero; extensions use these bits)
        20 bytes: info_hash
        20 bytes: peer_id
    """
    if len(info_hash) != 20:
        raise ValueError(f"info_hash must be 20 bytes, got {len(info_hash)}")
    if len(peer_id) != 20:
        raise ValueError(f"peer_id must be 20 bytes, got {len(peer_id)}")

    return (
        bytes([len(PROTOCOL_NAME)])
        + PROTOCOL_NAME
        + b"\x00" * 8          # reserved bytes
        + info_hash
        + peer_id
    )


def parse_handshake(data: bytes) -> tuple[bytes, bytes]:
    """
    Parse a 68-byte handshake and return (info_hash, peer_id).

    Raises:
        HandshakeError: if the handshake is malformed or the protocol string
                        doesn't match.
    """
    if len(data) < HANDSHAKE_LEN:
        raise HandshakeError(
            f"Handshake too short: expected {HANDSHAKE_LEN} bytes, got {len(data)}"
        )

    pstrlen = data[0]
    if pstrlen != len(PROTOCOL_NAME):
        raise HandshakeError(f"Unexpected protocol name length: {pstrlen}")

    pstr = data[1 : 1 + pstrlen]
    if pstr != PROTOCOL_NAME:
        raise HandshakeError(
            f"Protocol name mismatch: expected {PROTOCOL_NAME!r}, got {pstr!r}"
        )

    # [1 + 19 + 8 = 28] info_hash starts at byte 28
    info_hash = data[28:48]
    peer_id   = data[48:68]
    return info_hash, peer_id


# ---------------------------------------------------------------------------
# PeerConnection — async TCP lifecycle
# ---------------------------------------------------------------------------

class PeerConnection:
    """
    Manages one async TCP connection to a single BitTorrent peer.

    Lifecycle:
        conn = PeerConnection()
        await conn.connect("1.2.3.4", 6881, info_hash, peer_id)

        # After connect, the handshake is already done.
        # The first message is usually a Bitfield.
        msg = await conn.receive_message()

        await conn.send_message(InterestedMessage())
        msg = await conn.receive_message()  # expect Unchoke

        await conn.send_message(RequestMessage(index=0, begin=0))
        piece = await conn.receive_message()  # PieceMessage

        await conn.close()
    """

    CONNECT_TIMEOUT  = 10   # seconds to establish TCP connection
    HANDSHAKE_TIMEOUT = 10  # seconds to complete handshake
    READ_TIMEOUT      = 30  # seconds to wait for next message

    def __init__(self) -> None:
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self.peer_id:   Optional[bytes] = None  # filled in after handshake
        self.ip:   str = ""
        self.port: int = 0
        self.am_choking:      bool = True   # we are choking the peer
        self.am_interested:   bool = False  # we are interested in the peer
        self.peer_choking:    bool = True   # peer is choking us
        self.peer_interested: bool = False  # peer is interested in us

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(
        self,
        ip: str,
        port: int,
        info_hash: bytes,
        peer_id: bytes,
    ) -> None:
        """
        Open a TCP connection to the peer and complete the handshake.

        Raises:
            PeerError: on timeout, connection refused, or handshake mismatch.
        """
        self.ip   = ip
        self.port = port

        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=self.CONNECT_TIMEOUT,
            )
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as exc:
            raise PeerError(f"Cannot connect to {ip}:{port}: {exc}") from exc

        await self._do_handshake(info_hash, peer_id)

    async def _do_handshake(self, info_hash: bytes, peer_id: bytes) -> None:
        """Send our handshake and validate the peer's response."""
        hs = build_handshake(info_hash, peer_id)
        self._writer.write(hs)
        await self._writer.drain()

        try:
            response = await asyncio.wait_for(
                self._reader.readexactly(HANDSHAKE_LEN),
                timeout=self.HANDSHAKE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise HandshakeError(f"Handshake timeout with {self.ip}:{self.port}")
        except asyncio.IncompleteReadError as exc:
            raise HandshakeError(
                f"Peer {self.ip}:{self.port} closed connection during handshake"
            ) from exc
        except (ConnectionResetError, OSError) as exc:
            raise HandshakeError(
                f"Connection error with {self.ip}:{self.port} during handshake: {exc}"
            ) from exc

        remote_info_hash, remote_peer_id = parse_handshake(response)

        if remote_info_hash != info_hash:
            raise HandshakeError(
                f"info_hash mismatch: expected {info_hash.hex()}, "
                f"got {remote_info_hash.hex()}"
            )

        self.peer_id = remote_peer_id

    async def close(self) -> None:
        """Gracefully close the TCP connection."""
        if self._writer and not self._writer.is_closing():
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
        self._reader = None
        self._writer = None

    @property
    def is_connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    async def send_message(self, message: Message) -> None:
        """
        Encode and write a message to the TCP stream.

        Raises:
            PeerError: if the connection is closed or write fails.
        """
        if not self.is_connected:
            raise PeerError("Cannot send: not connected")

        data = message.encode()
        try:
            self._writer.write(data)
            await self._writer.drain()
        except (OSError, ConnectionResetError) as exc:
            raise PeerError(f"Send failed to {self.ip}:{self.port}: {exc}") from exc

        # Update local state for choke/interest messages
        if isinstance(message, ChokeMessage):
            self.am_choking = True
        elif isinstance(message, UnchokeMessage):
            self.am_choking = False
        elif isinstance(message, InterestedMessage):
            self.am_interested = True
        elif isinstance(message, NotInterestedMessage):
            self.am_interested = False

    # ------------------------------------------------------------------
    # Receiving
    # ------------------------------------------------------------------

    async def receive_message(self) -> Message:
        """
        Read the next message from the stream.

        Returns a typed Message object.

        Raises:
            PeerError: on disconnect, timeout, or protocol violation.
        """
        if not self.is_connected:
            raise PeerError("Cannot receive: not connected")

        try:
            # Read 4-byte length prefix
            length_bytes = await asyncio.wait_for(
                self._reader.readexactly(4),
                timeout=self.READ_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise PeerError(f"Read timeout from {self.ip}:{self.port}")
        except asyncio.IncompleteReadError:
            raise PeerError(f"Peer {self.ip}:{self.port} closed the connection")

        (length,) = struct.unpack("!I", length_bytes)

        # Keep-alive: length == 0, no message ID
        if length == 0:
            return KeepAliveMessage()

        # Read 1-byte message ID + payload
        try:
            body = await asyncio.wait_for(
                self._reader.readexactly(length),
                timeout=self.READ_TIMEOUT,
            )
        except asyncio.IncompleteReadError:
            raise PeerError(
                f"Peer {self.ip}:{self.port} closed mid-message (expected {length} bytes)"
            )

        msg_id  = body[0]
        payload = body[1:]
        msg     = decode_message(msg_id, payload)

        # Update state from received control messages
        if isinstance(msg, ChokeMessage):
            self.peer_choking = True
        elif isinstance(msg, UnchokeMessage):
            self.peer_choking = False
        elif isinstance(msg, InterestedMessage):
            self.peer_interested = True
        elif isinstance(msg, NotInterestedMessage):
            self.peer_interested = False

        return msg

    def __repr__(self) -> str:
        state = "connected" if self.is_connected else "disconnected"
        return f"PeerConnection({self.ip}:{self.port} [{state}])"