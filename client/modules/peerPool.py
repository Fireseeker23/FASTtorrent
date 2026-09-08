"""
peerPool.py — Concurrent peer connection manager.

Manages up to `max_peers` simultaneous BitTorrent peer connections and
coordinates them with PieceManager to drive the download loop.

Responsibilities:
  - Connect to peers returned by the tracker (respecting max_peers)
  - Run one asyncio Task per peer that handles the full wire protocol loop:
      handshake → bitfield → interested → unchoke → request → piece → …
  - Feed received blocks into PieceManager and trigger piece-complete callbacks
  - Handle choke/unchoke, Have messages, and disconnects gracefully
  - Allow the orchestrator to add fresh peers mid-download (re-announce)
  - Shut down all connections cleanly
"""

import asyncio
import logging
from asyncio import Task
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

from modules.peerProtocol import (
    BLOCK_SIZE,
    BitfieldMessage,
    CancelMessage,
    ChokeMessage,
    HaveMessage,
    InterestedMessage,
    KeepAliveMessage,
    NotInterestedMessage,
    PeerConnection,
    PeerError,
    PieceMessage,
    RequestMessage,
    UnchokeMessage,
)
from modules.pieceManager import PieceManager, PieceState

logger = logging.getLogger(__name__)

# Type alias for the piece-complete callback
# Called with (piece_index, piece_data) once a piece is verified
PieceCallback = Callable[[int, bytes], Awaitable[None]]


# ---------------------------------------------------------------------------
# PeerSession — encapsulates one peer's runtime state
# ---------------------------------------------------------------------------

class PeerSession:
    """
    Wraps a PeerConnection with download-loop state for one peer.
    """

    # How many block requests to pipeline per peer (reduces round-trip wait)
    PIPELINE_DEPTH = 5
    # Seconds of silence before considering a peer stalled
    STALL_TIMEOUT = 30

    def __init__(self, conn: PeerConnection) -> None:
        self.conn = conn

        # Pending requests we've sent but not yet received
        self._in_flight: Set[Tuple[int, int]] = set()  # (piece_index, offset)
        self._active_pieces: List[int] = []

    @property
    def key(self) -> str:
        return f"{self.conn.ip}:{self.conn.port}"

    def __repr__(self) -> str:
        return f"PeerSession({self.key})"


# ---------------------------------------------------------------------------
# PeerPoolManager
# ---------------------------------------------------------------------------

class PeerPoolManager:
    """
    Manages multiple concurrent peer connections and drives the download.

    Usage::

        pool = PeerPoolManager(
            piece_manager=mgr,
            info_hash=torrent.info_hash,
            peer_id=my_peer_id,
            on_piece_complete=file_writer.write_piece,
            max_peers=50,
        )
        await pool.add_peers(tracker_response.peers)
        await pool.wait_until_done()
        await pool.shutdown()
    """

    def __init__(
        self,
        piece_manager: PieceManager,
        info_hash: bytes,
        peer_id: bytes,
        on_piece_complete: PieceCallback,
        max_peers: int = 50,
    ) -> None:
        """
        Args:
            piece_manager:     Shared piece state tracker.
            info_hash:         20-byte torrent info_hash (for handshake).
            peer_id:           Our 20-byte peer ID.
            on_piece_complete: Async callback(piece_index, data) called when a
                               piece is verified and ready to write to disk.
            max_peers:         Maximum simultaneous connections.
        """
        self._pm               = piece_manager
        self._info_hash        = info_hash
        self._peer_id          = peer_id
        self._on_piece_complete = on_piece_complete
        self._max_peers        = max_peers

        # Active sessions: peer key → PeerSession
        self._sessions: Dict[str, PeerSession] = {}

        # asyncio Tasks, one per peer
        self._tasks: Dict[str, Task] = {}

        # Lock protecting _sessions and _tasks mutations
        self._lock = asyncio.Lock()

        # Fires when is_done becomes True
        self._done_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def add_peers(self, peers) -> None:
        """
        Attempt to connect to each PeerInfo in *peers*.

        Peers beyond max_peers or already connected are silently skipped.
        Each successful connection spawns an asyncio Task for the download loop.

        Args:
            peers: Iterable of objects with `.ip` and `.port` attributes
                   (e.g. ``trackCommunication.PeerInfo``).
        """
        async def _connect_one(peer_info) -> None:
            async with self._lock:
                if len(self._sessions) >= self._max_peers:
                    return
                key = f"{peer_info.ip}:{peer_info.port}"
                if key in self._sessions:
                    return

            conn = PeerConnection()
            try:
                await conn.connect(
                    peer_info.ip,
                    peer_info.port,
                    self._info_hash,
                    self._peer_id,
                )
            except PeerError as exc:
                logger.debug("Cannot connect to %s:%s: %s", peer_info.ip, peer_info.port, exc)
                return
            except Exception as exc:
                logger.debug("Connection error %s:%s: %s", peer_info.ip, peer_info.port, exc)
                return

            session = PeerSession(conn)
            async with self._lock:
                if len(self._sessions) >= self._max_peers:
                    await conn.close()
                    return
                self._sessions[key] = session

            task = asyncio.create_task(
                self._run_peer(session),
                name=f"peer-{key}",
            )
            async with self._lock:
                self._tasks[key] = task

            logger.info("Connected to peer %s", key)

        await asyncio.gather(*[_connect_one(p) for p in peers], return_exceptions=True)

    async def wait_until_done(self) -> None:
        """Block until all pieces have been downloaded and verified."""
        await self._done_event.wait()

    async def shutdown(self) -> None:
        """Cancel all peer tasks and close all connections."""
        async with self._lock:
            tasks    = list(self._tasks.values())
            sessions = list(self._sessions.values())

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for session in sessions:
            await session.conn.close()

        async with self._lock:
            self._sessions.clear()
            self._tasks.clear()

        logger.info("PeerPoolManager shut down")

    @property
    def num_connected(self) -> int:
        return len(self._sessions)

    @property
    def is_done(self) -> bool:
        return self._pm.is_done

    # ------------------------------------------------------------------
    # Per-peer download loop
    # ------------------------------------------------------------------

    async def _run_peer(self, session: PeerSession) -> None:
        """
        Main coroutine for one peer. Runs until the download is done,
        the peer misbehaves, or we are shut down.
        """
        key = session.key
        try:
            await self._peer_loop(session)
        except asyncio.CancelledError:
            pass
        except (PeerError, OSError, ConnectionError) as exc:
            logger.debug("Peer %s network/protocol error: %s", key, exc)
        except Exception as exc:
            logger.warning("Peer %s unexpected error: %s", key, exc, exc_info=True)
        finally:
            await self._disconnect(session)

    async def _peer_loop(self, session: PeerSession) -> None:
        """
        Wire protocol loop for one peer:
          1. Receive bitfield (optional — some peers skip it)
          2. Send Interested
          3. Wait for Unchoke
          4. Pipeline block Requests
          5. Feed received Piece blocks into PieceManager
        """
        conn = session.conn

        try:
            # Express interest immediately
            await conn.send_message(InterestedMessage())

            # Receive initial messages until unchoked (bitfield, have, unchoke in any order)
            unchoked = False
            for _ in range(25):
                try:
                    msg = await asyncio.wait_for(conn.receive_message(), timeout=15)
                except asyncio.TimeoutError:
                    break

                if isinstance(msg, UnchokeMessage):
                    unchoked = True
                    break
                elif isinstance(msg, BitfieldMessage):
                    self._pm.on_peer_bitfield(conn.peer_id or session.key.encode(), msg.bitfield)
                    logger.debug("Peer %s: got bitfield", session.key)
                elif isinstance(msg, HaveMessage):
                    self._pm.on_peer_have(conn.peer_id or session.key.encode(), msg.piece_index)
                elif isinstance(msg, ChokeMessage):
                    pass
                elif isinstance(msg, KeepAliveMessage):
                    pass

            if not unchoked:
                logger.debug("Peer %s never unchoked us, dropping", session.key)
                return

            # Request/receive download loop
            await self._download_loop(session)

        except PeerError as exc:
            logger.debug("Peer %s ended/protocol error: %s", session.key, exc)
            return

    async def _download_loop(self, session: PeerSession) -> None:
        """
        Pipeline block requests to a peer and process incoming messages.
        """
        conn     = session.conn
        peer_key = (conn.peer_id or session.key.encode())

        while not self._pm.is_done:
            # Fill the request pipeline
            while len(session._in_flight) < PeerSession.PIPELINE_DEPTH:
                requested_any = False

                # 1. Try to request next unrequested block from already active pieces
                for piece_idx in list(session._active_pieces):
                    piece = self._pm._pieces[piece_idx]
                    missing = piece.missing_blocks
                    unrequested = [
                        (off, sz) for off, sz in missing
                        if (piece_idx, off) not in session._in_flight
                    ]
                    if unrequested:
                        off, sz = unrequested[0]
                        await conn.send_message(RequestMessage(
                            index=piece_idx,
                            begin=off,
                            length=sz,
                        ))
                        session._in_flight.add((piece_idx, off))
                        requested_any = True
                        break

                if requested_any:
                    continue

                # 2. Need to select a new piece
                piece_idx = self._pm.select_piece(peer_key)
                if piece_idx is None:
                    break   # nothing left to request from this peer right now

                session._active_pieces.append(piece_idx)
                piece = self._pm._pieces[piece_idx]
                missing = piece.missing_blocks
                unrequested = [
                    (off, sz) for off, sz in missing
                    if (piece_idx, off) not in session._in_flight
                ]
                if unrequested:
                    off, sz = unrequested[0]
                    await conn.send_message(RequestMessage(
                        index=piece_idx,
                        begin=off,
                        length=sz,
                    ))
                    session._in_flight.add((piece_idx, off))
                else:
                    break

            if not session._in_flight:
                if self._pm.is_done:
                    break
                # Wait for next incoming message from peer (e.g. Have, Bitfield)
                try:
                    msg = await asyncio.wait_for(conn.receive_message(), timeout=5)
                    if isinstance(msg, HaveMessage):
                        self._pm.on_peer_have(peer_key, msg.piece_index)
                    elif isinstance(msg, BitfieldMessage):
                        self._pm.on_peer_bitfield(peer_key, msg.bitfield)
                    elif isinstance(msg, ChokeMessage):
                        return
                except asyncio.TimeoutError:
                    pass
                continue

            # Receive next message (with stall timeout)
            try:
                msg = await asyncio.wait_for(
                    conn.receive_message(),
                    timeout=PeerSession.STALL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.debug("Peer %s stalled, dropping", session.key)
                return

            if isinstance(msg, PieceMessage):
                session._in_flight.discard((msg.index, msg.begin))
                done = self._pm.add_block(msg.index, msg.begin, msg.block)
                if done:
                    if msg.index in session._active_pieces:
                        session._active_pieces.remove(msg.index)
                    piece_data = self._pm.get_piece_data(msg.index)
                    await self._on_piece_done(msg.index, piece_data)
                elif self._pm._pieces[msg.index].state == PieceState.NEEDED:
                    # Verification failed and piece was reset
                    if msg.index in session._active_pieces:
                        session._active_pieces.remove(msg.index)

            elif isinstance(msg, ChokeMessage):
                logger.debug("Peer %s choked us", session.key)
                # Cancel in-flight and active requests
                pieces_to_release = set(session._active_pieces)
                for piece_idx, _ in session._in_flight:
                    pieces_to_release.add(piece_idx)
                for piece_idx in pieces_to_release:
                    if piece_idx < len(self._pm._pieces) and self._pm._pieces[piece_idx].state != PieceState.COMPLETE:
                        self._pm.mark_piece_failed(piece_idx)
                session._in_flight.clear()
                session._active_pieces.clear()
                return

            elif isinstance(msg, UnchokeMessage):
                pass   # fine — already unchoked

            elif isinstance(msg, HaveMessage):
                self._pm.on_peer_have(peer_key, msg.piece_index)

            elif isinstance(msg, KeepAliveMessage):
                pass

        # Done!
        if self._pm.is_done:
            self._done_event.set()

    async def _on_piece_done(self, piece_index: int, data: bytes) -> None:
        """Invoke the caller's piece-complete callback."""
        try:
            await self._on_piece_complete(piece_index, data)
        except Exception as exc:
            logger.error("on_piece_complete callback raised: %s", exc, exc_info=True)

    async def _disconnect(self, session: PeerSession) -> None:
        """Remove the session from the pool and close the connection."""
        key = session.key
        async with self._lock:
            self._sessions.pop(key, None)
            self._tasks.pop(key, None)

        # Release any pieces this peer was responsible for
        pieces_to_release = set(getattr(session, "_active_pieces", []))
        for piece_idx, _ in session._in_flight:
            pieces_to_release.add(piece_idx)

        for piece_idx in pieces_to_release:
            if piece_idx < len(self._pm._pieces) and self._pm._pieces[piece_idx].state != PieceState.COMPLETE:
                self._pm.mark_piece_failed(piece_idx)

        await session.conn.close()
        logger.debug("Disconnected from peer %s", key)

    def __repr__(self) -> str:
        return (
            f"PeerPoolManager("
            f"peers={self.num_connected}/{self._max_peers}, "
            f"progress={self._pm.get_progress():.1%})"
        )