"""
Tests for peerPool.py — PeerPoolManager concurrent download coordination.

Because PeerPoolManager runs asyncio Tasks internally, tests use a shared
event loop and mock PeerConnection objects to simulate peer behaviour.
"""

import asyncio
import hashlib
import struct
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.peerProtocol import (
    BLOCK_SIZE,
    BitfieldMessage,
    ChokeMessage,
    HaveMessage,
    InterestedMessage,
    KeepAliveMessage,
    PeerError,
    PieceMessage,
    RequestMessage,
    UnchokeMessage,
    build_handshake,
)
from modules.pieceManager import PieceManager
from modules.peerPool import PeerPoolManager, PeerSession


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_INFO_HASH = b"\xaa" * 20
FAKE_PEER_ID   = b"-AG0001-" + b"x" * 12
BLOCK          = BLOCK_SIZE    # shorthand
NUM_PIECES     = 3
PIECE_LEN      = BLOCK * 2    # 2 blocks per piece


def _sha1(data: bytes) -> bytes:
    return hashlib.sha1(data).digest()


def _make_piece_data(fill: int = 0xAB) -> bytes:
    return bytes([fill]) * PIECE_LEN


def _make_manager() -> tuple[PieceManager, list[bytes]]:
    """Return a fresh PieceManager and the correct piece data."""
    pieces = [_make_piece_data(0xAB + i) for i in range(NUM_PIECES)]
    hashes = [_sha1(d) for d in pieces]
    mgr    = PieceManager(
        piece_hashes=hashes,
        piece_length=PIECE_LEN,
        total_length=PIECE_LEN * NUM_PIECES,
    )
    return mgr, pieces


def _make_pool(mgr, callback=None) -> PeerPoolManager:
    if callback is None:
        async def _noop(idx, data): pass
        callback = _noop
    return PeerPoolManager(
        piece_manager=mgr,
        info_hash=FAKE_INFO_HASH,
        peer_id=FAKE_PEER_ID,
        on_piece_complete=callback,
        max_peers=5,
    )


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


def _make_peer_info(ip: str = "1.2.3.4", port: int = 6881) -> MagicMock:
    pi = MagicMock()
    pi.ip   = ip
    pi.port = port
    return pi


def _make_connected_conn(peer_id: bytes = b"\xbb" * 20) -> MagicMock:
    """Build a mock PeerConnection that looks already-connected."""
    conn           = MagicMock()
    conn.ip        = "1.2.3.4"
    conn.port      = 6881
    conn.peer_id   = peer_id
    conn.is_connected = True
    conn.am_choking   = True
    conn.am_interested = False
    conn.peer_choking  = True
    conn.peer_interested = False
    conn.close         = AsyncMock()
    conn.send_message  = AsyncMock()
    return conn


def _make_session(conn=None) -> PeerSession:
    if conn is None:
        conn = _make_connected_conn()
    return PeerSession(conn)


# ---------------------------------------------------------------------------
# PeerSession
# ---------------------------------------------------------------------------

class TestPeerSession(unittest.TestCase):

    def test_key_format(self):
        conn      = _make_connected_conn()
        conn.ip   = "5.5.5.5"
        conn.port = 9999
        session   = PeerSession(conn)
        self.assertEqual(session.key, "5.5.5.5:9999")

    def test_repr(self):
        session = _make_session()
        self.assertIn("1.2.3.4:6881", repr(session))

    def test_in_flight_starts_empty(self):
        session = _make_session()
        self.assertEqual(len(session._in_flight), 0)


# ---------------------------------------------------------------------------
# PeerPoolManager — construction
# ---------------------------------------------------------------------------

class TestPeerPoolManagerConstruction(unittest.TestCase):

    def test_initial_state(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        self.assertEqual(pool.num_connected, 0)
        self.assertFalse(pool.is_done)

    def test_repr(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        r = repr(pool)
        self.assertIn("0/5", r)

    def test_is_done_when_pm_done(self):
        mgr, pieces = _make_manager()
        # Complete all pieces manually
        for i, data in enumerate(pieces):
            offset = 0
            while offset < len(data):
                mgr.add_block(i, offset, data[offset:offset + BLOCK])
                offset += BLOCK
        pool = _make_pool(mgr)
        self.assertTrue(pool.is_done)


# ---------------------------------------------------------------------------
# PeerPoolManager.add_peers — connection gating
# ---------------------------------------------------------------------------

class TestAddPeers(unittest.TestCase):

    def test_add_peers_respects_max(self):
        """Only max_peers connections should be made."""
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        pool._max_peers = 2

        peer_infos = [_make_peer_info(f"10.0.0.{i}", 6881) for i in range(5)]

        # Patch PeerConnection.connect to always succeed
        async def fake_connect(ip, port, ih, pid):
            pass

        conn_mock = _make_connected_conn()
        conn_mock.connect = AsyncMock(side_effect=fake_connect)

        with patch("modules.peerPool.PeerConnection", return_value=conn_mock):
            _run(pool.add_peers(peer_infos))

        self.assertLessEqual(pool.num_connected, 2)

    def test_add_peers_skips_already_connected(self):
        """Duplicate ip:port should not create a second session."""
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)

        # Pre-populate sessions with the same key
        session = _make_session()
        pool._sessions["1.2.3.4:6881"] = session

        peer_info = _make_peer_info("1.2.3.4", 6881)

        with patch("modules.peerPool.PeerConnection") as MockConn:
            _run(pool.add_peers([peer_info]))
            MockConn.assert_not_called()   # no new connection attempted

    def test_failed_connection_skipped(self):
        """Peers that refuse connection should be silently skipped."""
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)

        conn_mock = _make_connected_conn()
        conn_mock.connect = AsyncMock(side_effect=PeerError("refused"))

        with patch("modules.peerPool.PeerConnection", return_value=conn_mock):
            _run(pool.add_peers([_make_peer_info()]))

        self.assertEqual(pool.num_connected, 0)


# ---------------------------------------------------------------------------
# PeerPoolManager.shutdown
# ---------------------------------------------------------------------------

class TestShutdown(unittest.TestCase):

    def test_shutdown_closes_all_connections(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)

        # Manually add two sessions
        conn_a = _make_connected_conn()
        conn_b = _make_connected_conn()
        conn_b.ip   = "2.2.2.2"
        conn_b.port = 9999

        pool._sessions["1.2.3.4:6881"] = PeerSession(conn_a)
        pool._sessions["2.2.2.2:9999"] = PeerSession(conn_b)

        _run(pool.shutdown())

        conn_a.close.assert_called_once()
        conn_b.close.assert_called_once()

    def test_shutdown_clears_sessions(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        conn   = _make_connected_conn()
        pool._sessions["1.2.3.4:6881"] = PeerSession(conn)

        _run(pool.shutdown())

        self.assertEqual(pool.num_connected, 0)


# ---------------------------------------------------------------------------
# PeerPoolManager._disconnect
# ---------------------------------------------------------------------------

class TestDisconnect(unittest.TestCase):

    def test_disconnect_removes_session(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        session = _make_session()
        pool._sessions[session.key] = session

        _run(pool._disconnect(session))

        self.assertNotIn(session.key, pool._sessions)
        session.conn.close.assert_called_once()

    def test_disconnect_marks_in_flight_pieces_failed(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        session = _make_session()

        # Pretend piece 0 was in-flight
        mgr._pending.add(0)
        session._in_flight.add((0, 0))

        pool._sessions[session.key] = session
        _run(pool._disconnect(session))

        # Piece 0 should be reset to NEEDED
        from modules.pieceManager import PieceState
        self.assertEqual(mgr._pieces[0].state, PieceState.NEEDED)


# ---------------------------------------------------------------------------
# PeerPoolManager._peer_loop — download scenarios (fully mocked)
# ---------------------------------------------------------------------------

def _message_queue(*messages):
    """
    Return an async `receive_message` that yields each message in turn,
    then raises PeerError("done") to terminate the loop.
    """
    queue = list(messages)

    async def _recv():
        if queue:
            return queue.pop(0)
        raise PeerError("test stream ended")

    return _recv


class TestPeerLoop(unittest.TestCase):

    def _make_session_with_messages(self, *messages):
        conn = _make_connected_conn()
        conn.receive_message = _message_queue(*messages)
        return PeerSession(conn)

    # ── Bitfield then successful unchoke path ─────────────────────────

    def test_bitfield_registered_on_peer_loop(self):
        """Bitfield message should update the piece manager."""
        mgr, pieces = _make_manager()
        pool        = _make_pool(mgr)

        # 3 pieces → 1 byte bitfield; 0b11100000 = 0xe0 → pieces 0,1,2
        bf_bytes = b"\xe0"
        session = self._make_session_with_messages(
            BitfieldMessage(bitfield=bf_bytes),
            ChokeMessage(),    # immediately choked → loop exits
        )
        # Use a predictable peer_id bytes key
        peer_key = session.conn.peer_id

        async def run():
            await pool._peer_loop(session)

        _run(run())

        # on_peer_bitfield uses conn.peer_id as the key
        for i in range(3):
            self.assertTrue(
                mgr.peer_has_piece(peer_key, i),
                f"Expected peer to have piece {i}"
            )

    def test_interested_sent_after_bitfield(self):
        mgr, _  = _make_manager()
        pool    = _make_pool(mgr)
        session = self._make_session_with_messages(
            BitfieldMessage(bitfield=b"\xe0"),
            ChokeMessage(),
        )

        async def run():
            await pool._peer_loop(session)

        _run(run())

        calls = [type(c.args[0]) for c in session.conn.send_message.call_args_list]
        self.assertIn(InterestedMessage, calls)

    def test_have_message_updates_peer_pieces(self):
        mgr, _ = _make_manager()
        pool   = _make_pool(mgr)
        session = self._make_session_with_messages(
            HaveMessage(piece_index=1),   # instead of bitfield
            ChokeMessage(),
        )

        async def run():
            await pool._peer_loop(session)

        _run(run())

        peer_key = session.conn.peer_id
        self.assertTrue(mgr.peer_has_piece(peer_key, 1))

    def test_unchoke_triggers_download_loop(self):
        """After unchoke, at least one RequestMessage should be sent."""
        mgr, pieces = _make_manager()
        pool        = _make_pool(mgr)

        bf_bytes = b"\xe0"  # peer has all 3 pieces

        # Build piece responses: for each piece we deliver both blocks
        piece_messages = []
        for piece_idx, data in enumerate(pieces):
            piece_messages.append(
                PieceMessage(index=piece_idx, begin=0,     block=data[:BLOCK])
            )
            piece_messages.append(
                PieceMessage(index=piece_idx, begin=BLOCK, block=data[BLOCK:])
            )

        session = self._make_session_with_messages(
            BitfieldMessage(bitfield=bf_bytes),
            UnchokeMessage(),
            *piece_messages,
        )

        async def run():
            await pool._peer_loop(session)

        _run(run())

        # At least one RequestMessage was sent
        request_calls = [
            c for c in session.conn.send_message.call_args_list
            if isinstance(c.args[0], RequestMessage)
        ]
        self.assertGreater(len(request_calls), 0)

    def test_full_download_completes(self):
        """
        Simulate a peer that delivers all pieces → pool marks done.
        We set PIPELINE_DEPTH=1 so requests are strictly one-at-a-time,
        matching our message queue exactly.
        """
        mgr, pieces = _make_manager()
        received    = []

        async def on_complete(idx, data):
            received.append((idx, data))

        pool = PeerPoolManager(
            piece_manager=mgr,
            info_hash=FAKE_INFO_HASH,
            peer_id=FAKE_PEER_ID,
            on_piece_complete=on_complete,
            max_peers=5,
        )

        bf_bytes = b"\xe0"  # peer has pieces 0, 1, 2

        piece_messages = []
        for piece_idx, data in enumerate(pieces):
            piece_messages.append(PieceMessage(index=piece_idx, begin=0,     block=data[:BLOCK]))
            piece_messages.append(PieceMessage(index=piece_idx, begin=BLOCK, block=data[BLOCK:]))

        session = self._make_session_with_messages(
            BitfieldMessage(bitfield=bf_bytes),
            UnchokeMessage(),
            *piece_messages,
        )

        # Reduce pipeline depth to 1 so we never request more than one block
        # at a time — this prevents the loop from consuming more queue slots
        # than we have messages.
        original_depth = PeerSession.PIPELINE_DEPTH
        PeerSession.PIPELINE_DEPTH = 1
        try:
            async def run():
                await pool._peer_loop(session)
            _run(run())
        finally:
            PeerSession.PIPELINE_DEPTH = original_depth

        self.assertTrue(mgr.is_done)
        self.assertEqual(len(received), NUM_PIECES)

    def test_choke_mid_download_releases_in_flight(self):
        """A choke during download must reset in-flight pieces."""
        mgr, pieces = _make_manager()
        pool        = _make_pool(mgr)

        bf_bytes = b"\xe0"
        session  = self._make_session_with_messages(
            BitfieldMessage(bitfield=bf_bytes),
            UnchokeMessage(),
            ChokeMessage(),   # immediately choked after unchoke
        )
        session._in_flight.add((0, 0))   # pretend we already requested

        async def run():
            await pool._peer_loop(session)

        _run(run())

        # in-flight should have been cleared
        self.assertEqual(len(session._in_flight), 0)

    def test_keepalive_ignored(self):
        """KeepAlive should not crash the loop."""
        mgr, pieces = _make_manager()
        pool        = _make_pool(mgr)
        session     = self._make_session_with_messages(
            KeepAliveMessage(),
            ChokeMessage(),
        )

        async def run():
            await pool._peer_loop(session)

        _run(run())


# ---------------------------------------------------------------------------
# PeerPoolManager._on_piece_done
# ---------------------------------------------------------------------------

class TestOnPieceDone(unittest.TestCase):

    def test_callback_invoked_with_correct_args(self):
        mgr, pieces = _make_manager()
        calls       = []

        async def cb(idx, data):
            calls.append((idx, data))

        pool = _make_pool(mgr, callback=cb)

        # Complete piece 0 so get_piece_data works
        offset = 0
        while offset < len(pieces[0]):
            mgr.add_block(0, offset, pieces[0][offset:offset + BLOCK])
            offset += BLOCK

        _run(pool._on_piece_done(0, mgr.get_piece_data(0)))

        self.assertEqual(len(calls), 1)
        idx, data = calls[0]
        self.assertEqual(idx, 0)
        self.assertEqual(data, pieces[0])

    def test_callback_exception_does_not_propagate(self):
        """A crashing callback must not kill the download loop."""
        mgr, _ = _make_manager()

        async def bad_cb(idx, data):
            raise RuntimeError("disk full")

        pool = _make_pool(mgr, callback=bad_cb)

        # Should not raise
        _run(pool._on_piece_done(0, b"data"))


if __name__ == "__main__":
    unittest.main()
