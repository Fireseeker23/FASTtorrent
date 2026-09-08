"""
Tests for peerProtocol.py — BitTorrent peer wire protocol (BEP-3).

Coverage:
  - Handshake building and parsing
  - Message encoding (every message type)
  - Message decoding (decode_message)
  - BitfieldMessage.has_piece
  - PeerConnection with mocked asyncio streams
"""

import asyncio
import struct
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.peerProtocol import (
    BLOCK_SIZE,
    HANDSHAKE_LEN,
    KEEP_ALIVE_BYTES,
    PROTOCOL_NAME,
    BitfieldMessage,
    CancelMessage,
    ChokeMessage,
    HandshakeError,
    HaveMessage,
    InterestedMessage,
    KeepAliveMessage,
    MessageID,
    NotInterestedMessage,
    PeerConnection,
    PeerError,
    PieceMessage,
    RequestMessage,
    UnchokeMessage,
    build_handshake,
    decode_message,
    parse_handshake,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_INFO_HASH = b"\xaa" * 20
FAKE_PEER_ID   = b"-AG0001-" + b"x" * 12   # 20 bytes


def _run(coro):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _make_framed(msg_id: int, payload: bytes = b"") -> bytes:
    """Manually build a framed message for comparison."""
    body = bytes([msg_id]) + payload
    return struct.pack("!I", len(body)) + body


def _make_stream_pair(read_data: bytes):
    """
    Return (reader_mock, writer_mock) backed by *read_data*.
    reader_mock.readexactly(n) consumes n bytes sequentially.
    """
    buf = bytearray(read_data)

    async def readexactly(n):
        if len(buf) < n:
            raise asyncio.IncompleteReadError(bytes(buf), n)
        data = bytes(buf[:n])
        del buf[:n]
        return data

    reader = MagicMock()
    reader.readexactly = readexactly

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    writer.is_closing = MagicMock(return_value=False)

    return reader, writer


# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------

class TestBuildHandshake(unittest.TestCase):

    def test_length_is_68(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(len(hs), HANDSHAKE_LEN)

    def test_pstrlen_byte(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(hs[0], len(PROTOCOL_NAME))  # 19

    def test_protocol_name(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(hs[1:20], PROTOCOL_NAME)

    def test_reserved_bytes_are_zero(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(hs[20:28], b"\x00" * 8)

    def test_info_hash_embedded(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(hs[28:48], FAKE_INFO_HASH)

    def test_peer_id_embedded(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        self.assertEqual(hs[48:68], FAKE_PEER_ID)

    def test_wrong_info_hash_length_raises(self):
        with self.assertRaises(ValueError):
            build_handshake(b"\xaa" * 19, FAKE_PEER_ID)

    def test_wrong_peer_id_length_raises(self):
        with self.assertRaises(ValueError):
            build_handshake(FAKE_INFO_HASH, b"short")


class TestParseHandshake(unittest.TestCase):

    def test_roundtrip(self):
        hs = build_handshake(FAKE_INFO_HASH, FAKE_PEER_ID)
        info_hash, peer_id = parse_handshake(hs)
        self.assertEqual(info_hash, FAKE_INFO_HASH)
        self.assertEqual(peer_id, FAKE_PEER_ID)

    def test_too_short_raises(self):
        with self.assertRaises(HandshakeError):
            parse_handshake(b"\x13" + PROTOCOL_NAME)   # missing reserved+hashes

    def test_wrong_protocol_raises(self):
        bad = (
            bytes([19])
            + b"WrongProtocol!!!!!!!"   # 20 chars — make 19 to match pstrlen
        )
        # pstrlen=19 but "WrongProtocol!!!!!!!" is 20 chars — adjust
        bad = bytes([19]) + b"WrongProtocol!!!!!!" + b"\x00" * 8 + b"\xaa" * 20 + b"\xbb" * 20
        with self.assertRaises(HandshakeError):
            parse_handshake(bad)

    def test_mismatched_pstrlen_raises(self):
        bad = bytes([10]) + PROTOCOL_NAME + b"\x00" * 8 + b"\xaa" * 20 + b"\xbb" * 20
        with self.assertRaises(HandshakeError):
            parse_handshake(bad)


# ---------------------------------------------------------------------------
# Message encoding
# ---------------------------------------------------------------------------

class TestKeepAliveMessage(unittest.TestCase):

    def test_encode(self):
        msg = KeepAliveMessage()
        self.assertEqual(msg.encode(), KEEP_ALIVE_BYTES)

    def test_repr(self):
        self.assertIn("KeepAlive", repr(KeepAliveMessage()))


class TestChokeMessage(unittest.TestCase):

    def test_encode(self):
        self.assertEqual(ChokeMessage().encode(), _make_framed(MessageID.CHOKE))

    def test_length_field_is_1(self):
        data = ChokeMessage().encode()
        (length,) = struct.unpack("!I", data[:4])
        self.assertEqual(length, 1)  # 1 byte: just the ID


class TestUnchokeMessage(unittest.TestCase):

    def test_encode(self):
        self.assertEqual(UnchokeMessage().encode(), _make_framed(MessageID.UNCHOKE))


class TestInterestedMessage(unittest.TestCase):

    def test_encode(self):
        self.assertEqual(InterestedMessage().encode(), _make_framed(MessageID.INTERESTED))


class TestNotInterestedMessage(unittest.TestCase):

    def test_encode(self):
        self.assertEqual(NotInterestedMessage().encode(), _make_framed(MessageID.NOT_INTERESTED))


class TestHaveMessage(unittest.TestCase):

    def test_encode(self):
        msg = HaveMessage(piece_index=42)
        expected = _make_framed(MessageID.HAVE, struct.pack("!I", 42))
        self.assertEqual(msg.encode(), expected)

    def test_piece_index_zero(self):
        msg = HaveMessage(piece_index=0)
        data = msg.encode()
        payload = data[5:]  # skip 4-byte length + 1-byte ID
        (idx,) = struct.unpack("!I", payload)
        self.assertEqual(idx, 0)

    def test_repr_contains_index(self):
        self.assertIn("42", repr(HaveMessage(piece_index=42)))


class TestBitfieldMessage(unittest.TestCase):

    def test_encode(self):
        bf = b"\xff\x00"
        msg = BitfieldMessage(bitfield=bf)
        expected = _make_framed(MessageID.BITFIELD, bf)
        self.assertEqual(msg.encode(), expected)

    def test_has_piece_all_ones(self):
        msg = BitfieldMessage(bitfield=b"\xff")
        for i in range(8):
            self.assertTrue(msg.has_piece(i))

    def test_has_piece_all_zeros(self):
        msg = BitfieldMessage(bitfield=b"\x00")
        for i in range(8):
            self.assertFalse(msg.has_piece(i))

    def test_has_piece_specific_bits(self):
        # 0b10100000 = 0xa0 → pieces 0 and 2 are set
        msg = BitfieldMessage(bitfield=b"\xa0")
        self.assertTrue(msg.has_piece(0))
        self.assertFalse(msg.has_piece(1))
        self.assertTrue(msg.has_piece(2))
        self.assertFalse(msg.has_piece(3))

    def test_has_piece_out_of_range_returns_false(self):
        msg = BitfieldMessage(bitfield=b"\xff")
        self.assertFalse(msg.has_piece(100))

    def test_repr(self):
        msg = BitfieldMessage(bitfield=b"\xff" * 5)
        self.assertIn("5", repr(msg))


class TestRequestMessage(unittest.TestCase):

    def test_encode(self):
        msg = RequestMessage(index=3, begin=0, length=BLOCK_SIZE)
        payload = struct.pack("!III", 3, 0, BLOCK_SIZE)
        expected = _make_framed(MessageID.REQUEST, payload)
        self.assertEqual(msg.encode(), expected)

    def test_default_length_is_block_size(self):
        msg = RequestMessage(index=0, begin=0)
        self.assertEqual(msg.length, BLOCK_SIZE)

    def test_repr(self):
        msg = RequestMessage(index=5, begin=16384)
        r = repr(msg)
        self.assertIn("5", r)
        self.assertIn("16384", r)


class TestPieceMessage(unittest.TestCase):

    def test_encode(self):
        block = b"A" * 16384
        msg   = PieceMessage(index=1, begin=0, block=block)
        header  = struct.pack("!II", 1, 0)
        expected = _make_framed(MessageID.PIECE, header + block)
        self.assertEqual(msg.encode(), expected)

    def test_repr_shows_length(self):
        msg = PieceMessage(index=0, begin=0, block=b"x" * 100)
        self.assertIn("100", repr(msg))


class TestCancelMessage(unittest.TestCase):

    def test_encode(self):
        msg = CancelMessage(index=2, begin=32768, length=BLOCK_SIZE)
        payload = struct.pack("!III", 2, 32768, BLOCK_SIZE)
        expected = _make_framed(MessageID.CANCEL, payload)
        self.assertEqual(msg.encode(), expected)


# ---------------------------------------------------------------------------
# decode_message
# ---------------------------------------------------------------------------

class TestDecodeMessage(unittest.TestCase):

    def test_decode_choke(self):
        self.assertIsInstance(decode_message(MessageID.CHOKE, b""), ChokeMessage)

    def test_decode_unchoke(self):
        self.assertIsInstance(decode_message(MessageID.UNCHOKE, b""), UnchokeMessage)

    def test_decode_interested(self):
        self.assertIsInstance(decode_message(MessageID.INTERESTED, b""), InterestedMessage)

    def test_decode_not_interested(self):
        self.assertIsInstance(decode_message(MessageID.NOT_INTERESTED, b""), NotInterestedMessage)

    def test_decode_have(self):
        payload = struct.pack("!I", 7)
        msg = decode_message(MessageID.HAVE, payload)
        self.assertIsInstance(msg, HaveMessage)
        self.assertEqual(msg.piece_index, 7)

    def test_decode_have_bad_payload_raises(self):
        with self.assertRaises(PeerError):
            decode_message(MessageID.HAVE, b"\x00")   # only 1 byte, need 4

    def test_decode_bitfield(self):
        payload = b"\xff\xf0"
        msg = decode_message(MessageID.BITFIELD, payload)
        self.assertIsInstance(msg, BitfieldMessage)
        self.assertEqual(msg.bitfield, payload)

    def test_decode_request(self):
        payload = struct.pack("!III", 3, 0, BLOCK_SIZE)
        msg = decode_message(MessageID.REQUEST, payload)
        self.assertIsInstance(msg, RequestMessage)
        self.assertEqual(msg.index, 3)
        self.assertEqual(msg.begin, 0)
        self.assertEqual(msg.length, BLOCK_SIZE)

    def test_decode_request_bad_payload_raises(self):
        with self.assertRaises(PeerError):
            decode_message(MessageID.REQUEST, b"\x00" * 8)   # need 12

    def test_decode_piece(self):
        block = b"Z" * 512
        payload = struct.pack("!II", 5, 1024) + block
        msg = decode_message(MessageID.PIECE, payload)
        self.assertIsInstance(msg, PieceMessage)
        self.assertEqual(msg.index, 5)
        self.assertEqual(msg.begin, 1024)
        self.assertEqual(msg.block, block)

    def test_decode_piece_too_short_raises(self):
        with self.assertRaises(PeerError):
            decode_message(MessageID.PIECE, b"\x00" * 4)   # need >=8

    def test_decode_cancel(self):
        payload = struct.pack("!III", 1, 0, BLOCK_SIZE)
        msg = decode_message(MessageID.CANCEL, payload)
        self.assertIsInstance(msg, CancelMessage)

    def test_decode_unknown_id_raises(self):
        with self.assertRaises(PeerError):
            decode_message(99, b"")


# ---------------------------------------------------------------------------
# PeerConnection — mocked asyncio streams
# ---------------------------------------------------------------------------

class TestPeerConnectionHandshake(unittest.TestCase):

    def _build_peer_hs(self, info_hash=None, peer_id=None):
        """A valid handshake the 'remote peer' sends back."""
        return build_handshake(
            info_hash or FAKE_INFO_HASH,
            peer_id or b"\xbb" * 20,
        )

    def test_successful_connect(self):
        reader, writer = _make_stream_pair(self._build_peer_hs())

        async def fake_open(ip, port):
            return reader, writer

        conn = PeerConnection()
        with patch("modules.peerProtocol.asyncio.open_connection", side_effect=fake_open):
            _run(conn.connect("1.2.3.4", 6881, FAKE_INFO_HASH, FAKE_PEER_ID))

        # Handshake was written
        written = b"".join(call.args[0] for call in writer.write.call_args_list)
        self.assertEqual(len(written), HANDSHAKE_LEN)
        self.assertEqual(conn.peer_id, b"\xbb" * 20)
        self.assertTrue(conn.is_connected)

    def test_info_hash_mismatch_raises(self):
        wrong_hash  = b"\x00" * 20
        remote_hs   = build_handshake(wrong_hash, b"\xbb" * 20)
        reader, writer = _make_stream_pair(remote_hs)

        async def fake_open(ip, port):
            return reader, writer

        conn = PeerConnection()
        with patch("modules.peerProtocol.asyncio.open_connection", side_effect=fake_open):
            with self.assertRaises(HandshakeError):
                _run(conn.connect("1.2.3.4", 6881, FAKE_INFO_HASH, FAKE_PEER_ID))

    def test_connection_refused_raises(self):
        async def fail_open(ip, port):
            raise ConnectionRefusedError()

        conn = PeerConnection()
        with patch("modules.peerProtocol.asyncio.open_connection", side_effect=fail_open):
            with self.assertRaises(PeerError):
                _run(conn.connect("1.2.3.4", 9999, FAKE_INFO_HASH, FAKE_PEER_ID))

    def test_handshake_incomplete_read_raises(self):
        # peer closes mid-handshake
        async def fake_open(ip, port):
            reader = MagicMock()
            reader.readexactly = AsyncMock(
                side_effect=asyncio.IncompleteReadError(b"", HANDSHAKE_LEN)
            )
            writer = MagicMock()
            writer.write = MagicMock()
            writer.drain = AsyncMock()
            writer.is_closing = MagicMock(return_value=False)
            return reader, writer

        conn = PeerConnection()
        with patch("modules.peerProtocol.asyncio.open_connection", side_effect=fake_open):
            with self.assertRaises(HandshakeError):
                _run(conn.connect("1.2.3.4", 6881, FAKE_INFO_HASH, FAKE_PEER_ID))


class TestPeerConnectionSendReceive(unittest.TestCase):

    def _connected_conn(self, read_data: bytes = b""):
        """Return a PeerConnection already connected (mocked)."""
        reader, writer = _make_stream_pair(read_data)
        conn = PeerConnection()
        conn._reader = reader
        conn._writer = writer
        conn.ip   = "1.2.3.4"
        conn.port = 6881
        return conn, writer

    # ── send_message ──────────────────────────────────────────────────

    def test_send_choke(self):
        conn, writer = self._connected_conn()
        _run(conn.send_message(ChokeMessage()))
        written = b"".join(call.args[0] for call in writer.write.call_args_list)
        self.assertEqual(written, ChokeMessage().encode())

    def test_send_updates_am_choking(self):
        conn, _ = self._connected_conn()
        conn.am_choking = False
        _run(conn.send_message(ChokeMessage()))
        self.assertTrue(conn.am_choking)

    def test_send_unchoke_updates_state(self):
        conn, _ = self._connected_conn()
        _run(conn.send_message(UnchokeMessage()))
        self.assertFalse(conn.am_choking)

    def test_send_interested_updates_state(self):
        conn, _ = self._connected_conn()
        _run(conn.send_message(InterestedMessage()))
        self.assertTrue(conn.am_interested)

    def test_send_not_interested_updates_state(self):
        conn, _ = self._connected_conn()
        conn.am_interested = True
        _run(conn.send_message(NotInterestedMessage()))
        self.assertFalse(conn.am_interested)

    def test_send_on_disconnected_raises(self):
        conn = PeerConnection()   # never connected
        with self.assertRaises(PeerError):
            _run(conn.send_message(ChokeMessage()))

    # ── receive_message ───────────────────────────────────────────────

    def test_receive_keepalive(self):
        conn, _ = self._connected_conn(read_data=b"\x00\x00\x00\x00")
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, KeepAliveMessage)

    def test_receive_choke(self):
        conn, _ = self._connected_conn(read_data=_make_framed(MessageID.CHOKE))
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, ChokeMessage)
        self.assertTrue(conn.peer_choking)

    def test_receive_unchoke_updates_state(self):
        conn, _ = self._connected_conn(read_data=_make_framed(MessageID.UNCHOKE))
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, UnchokeMessage)
        self.assertFalse(conn.peer_choking)

    def test_receive_have(self):
        payload = struct.pack("!I", 12)
        conn, _ = self._connected_conn(read_data=_make_framed(MessageID.HAVE, payload))
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, HaveMessage)
        self.assertEqual(msg.piece_index, 12)

    def test_receive_bitfield(self):
        bf = b"\xff\x80"
        conn, _ = self._connected_conn(read_data=_make_framed(MessageID.BITFIELD, bf))
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, BitfieldMessage)
        self.assertEqual(msg.bitfield, bf)

    def test_receive_piece(self):
        block   = b"D" * 100
        payload = struct.pack("!II", 2, 0) + block
        conn, _ = self._connected_conn(read_data=_make_framed(MessageID.PIECE, payload))
        msg = _run(conn.receive_message())
        self.assertIsInstance(msg, PieceMessage)
        self.assertEqual(msg.index, 2)
        self.assertEqual(msg.block, block)

    def test_receive_on_disconnected_raises(self):
        conn = PeerConnection()
        with self.assertRaises(PeerError):
            _run(conn.receive_message())

    def test_receive_peer_closes_raises(self):
        # empty buffer → IncompleteReadError
        conn, _ = self._connected_conn(read_data=b"")
        with self.assertRaises(PeerError):
            _run(conn.receive_message())

    def test_receive_multiple_messages_in_sequence(self):
        """Reader is stateful — two messages consumed in order."""
        have_payload = struct.pack("!I", 5)
        stream = (
            _make_framed(MessageID.UNCHOKE)
            + _make_framed(MessageID.HAVE, have_payload)
        )
        conn, _ = self._connected_conn(read_data=stream)

        msg1 = _run(conn.receive_message())
        msg2 = _run(conn.receive_message())

        self.assertIsInstance(msg1, UnchokeMessage)
        self.assertIsInstance(msg2, HaveMessage)
        self.assertEqual(msg2.piece_index, 5)

    # ── close ─────────────────────────────────────────────────────────

    def test_close_marks_disconnected(self):
        conn, writer = self._connected_conn()
        _run(conn.close())
        writer.close.assert_called_once()
        self.assertIsNone(conn._writer)


class TestPeerConnectionRepr(unittest.TestCase):

    def test_repr_disconnected(self):
        conn = PeerConnection()
        r = repr(conn)
        self.assertIn("disconnected", r)

    def test_repr_connected(self):
        _, writer = _make_stream_pair(b"")
        conn = PeerConnection()
        conn._reader = MagicMock()
        conn._writer = writer
        conn.ip   = "5.5.5.5"
        conn.port = 1234
        r = repr(conn)
        self.assertIn("5.5.5.5", r)
        self.assertIn("connected", r)


if __name__ == "__main__":
    unittest.main()
