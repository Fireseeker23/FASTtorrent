
import asyncio
import ipaddress
import os
import struct
from dataclasses import dataclass, field
from typing import List, Literal, Optional
from urllib.parse import urlencode

import aiohttp

from modules.bencode import BencodeDecoder, BencodeDecodeError
from modules.torrentFileParser import TorrentFile


class TrackerError(Exception):
    """Raised when a tracker request fails or returns an error response."""
    pass




@dataclass
class PeerInfo:
    """A single peer returned by the tracker."""
    ip: str
    port: int

    def __repr__(self) -> str:
        return f"PeerInfo({self.ip}:{self.port})"


@dataclass
class TrackerResponse:

    interval: int
    peers: List[PeerInfo]
    min_interval: Optional[int] = None
    seeders: Optional[int] = None
    leechers: Optional[int] = None
    tracker_id: Optional[bytes] = None
    warning_message: Optional[str] = None

    @staticmethod
    def from_bencode(data: dict) -> "TrackerResponse":

        def _b(key: str):

            bkey = key.encode()
            if bkey in data:
                return data[bkey]
            return data.get(key)


        failure = _b("failure reason")
        if failure is not None:
            reason = failure.decode("utf-8", errors="replace") if isinstance(failure, bytes) else str(failure)
            raise TrackerError(f"Tracker failure: {reason}")

        warning_raw = _b("warning message")
        warning = (
            warning_raw.decode("utf-8", errors="replace")
            if isinstance(warning_raw, (bytes, bytearray))
            else (str(warning_raw) if warning_raw is not None else None)
        )

        interval = _b("interval")
        if interval is None:
            raise TrackerError("Tracker response missing 'interval'")

        min_interval = _b("min interval")
        seeders   = _b("complete")
        leechers  = _b("incomplete")
        tracker_id = _b("tracker id")

        peers_raw = _b("peers")
        if peers_raw is None:  # explicitly absent — not just falsy
            raise TrackerError("Tracker response missing 'peers'")

        peers = TrackerResponse._parse_peers(peers_raw)

        return TrackerResponse(
            interval=int(interval),
            peers=peers,
            min_interval=int(min_interval) if min_interval is not None else None,
            seeders=int(seeders) if seeders is not None else None,
            leechers=int(leechers) if leechers is not None else None,
            tracker_id=tracker_id if isinstance(tracker_id, bytes) else None,
            warning_message=warning,
        )

    @staticmethod
    def _parse_peers(peers_raw) -> List[PeerInfo]:

        if isinstance(peers_raw, (bytes, bytearray)):
            return TrackerResponse._parse_compact_peers(bytes(peers_raw))
        elif isinstance(peers_raw, list):
            return TrackerResponse._parse_dict_peers(peers_raw)
        else:
            raise TrackerError(f"Unexpected 'peers' type: {type(peers_raw).__name__}")

    @staticmethod
    def _parse_compact_peers(data: bytes) -> List[PeerInfo]:
        if len(data) % 6 != 0:
            raise TrackerError(
                f"Compact peers length {len(data)} is not a multiple of 6"
            )
        peers: List[PeerInfo] = []
        for i in range(0, len(data), 6):
            ip_int = struct.unpack_from("!I", data, i)[0]    # 4-byte big-endian uint
            port   = struct.unpack_from("!H", data, i + 4)[0]  # 2-byte big-endian ushort
            ip_str = str(ipaddress.IPv4Address(ip_int))
            peers.append(PeerInfo(ip=ip_str, port=port))
        return peers

    @staticmethod
    def _parse_dict_peers(peer_list: list) -> List[PeerInfo]:
        peers: List[PeerInfo] = []
        for entry in peer_list:
            if not isinstance(entry, dict):
                raise TrackerError(f"Non-compact peer entry is not a dict: {entry!r}")
            ip_raw   = entry.get(b"ip")   or entry.get("ip")
            port_raw = entry.get(b"port") or entry.get("port")
            if ip_raw is None or port_raw is None:
                raise TrackerError(f"Peer entry missing 'ip' or 'port': {entry!r}")
            ip = ip_raw.decode("utf-8", errors="replace") if isinstance(ip_raw, bytes) else str(ip_raw)
            peers.append(PeerInfo(ip=ip, port=int(port_raw)))
        return peers




class TrackerClient:


    DEFAULT_TIMEOUT  = aiohttp.ClientTimeout(total=15)

    def __init__(self, timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT) -> None:
        self._timeout = timeout


    async def announce(
        self,
        torrent: TorrentFile,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
        event: Optional[Literal["started", "stopped", "completed"]] = None,
        compact: int = 1,
        numwant: int = 50,
    ) -> TrackerResponse:

        params = self._build_params(
            info_hash=torrent.info_hash,
            peer_id=peer_id,
            port=port,
            uploaded=uploaded,
            downloaded=downloaded,
            left=left,
            event=event,
            compact=compact,
            numwant=numwant,
        )

        urls = self._build_url_list(torrent)
        if not urls:
            raise TrackerError(
                f"No HTTP/HTTPS trackers found for this torrent. "
                f"Primary tracker: {torrent.announce!r}. "
                f"This client only supports HTTP tracker protocol (BEP-3). "
                f"UDP and WebSocket trackers are not yet supported."
            )
        last_error: Exception = TrackerError("No tracker URLs available")

        for url in urls:
            try:
                return await self._do_announce(url, params)
            except TrackerError as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = TrackerError(f"Request to {url!r} failed: {exc}")
                continue

        raise last_error


    @staticmethod
    def _build_params(
        *,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
        event: Optional[str],
        compact: int,
        numwant: int,
    ) -> dict:
        """
        Build query parameters for the tracker announce.

        info_hash and peer_id are raw bytes and must be percent-encoded
        (e.g. each byte as %XX).  aiohttp cannot handle bytes as param
        values, so we store them pre-encoded as strings and build the
        final URL in _do_announce.
        """
        from urllib.parse import quote_from_bytes
        params: dict = {
            "_info_hash_raw": info_hash,   # kept for tests; encoded below
            "_peer_id_raw":   peer_id,
            "info_hash":      quote_from_bytes(info_hash, safe=""),
            "peer_id":        quote_from_bytes(peer_id,   safe=""),
            "port":           port,
            "uploaded":       uploaded,
            "downloaded":     downloaded,
            "left":           left,
            "compact":        compact,
            "numwant":        numwant,
        }
        if event is not None:
            params["event"] = event
        return params

    @staticmethod
    def _build_url_list(torrent: TorrentFile) -> List[str]:
        """
        Return HTTP/HTTPS tracker URLs to try, in order.

        UDP (udp://) and WebSocket (wss://) trackers are silently skipped —
        this client only supports the HTTP tracker protocol (BEP-3).
        """
        seen: set[str] = set()
        urls: List[str] = []

        def _add(url: str) -> None:
            if url and url.startswith(("http://", "https://")) and url not in seen:
                seen.add(url)
                urls.append(url)

        _add(torrent.announce)
        for tier in torrent.announce_list:
            for url in tier:
                _add(url)

        return urls

    async def _do_announce(self, base_url: str, params: dict) -> TrackerResponse:
        """
        Fire a GET request to *base_url* with *params* and parse the bencoded response.

        info_hash and peer_id are already percent-encoded strings in params.
        We build the query string manually so that aiohttp doesn't re-encode them.
        """
        from urllib.parse import urlencode

        # Extract pre-encoded binary fields; pass the rest to urlencode normally
        info_hash_encoded = params["info_hash"]
        peer_id_encoded   = params["peer_id"]

        plain_params = {
            k: v for k, v in params.items()
            if k not in ("info_hash", "peer_id", "_info_hash_raw", "_peer_id_raw")
        }
        qs = urlencode(plain_params)
        qs += f"&info_hash={info_hash_encoded}&peer_id={peer_id_encoded}"
        full_url = f"{base_url}?{qs}"

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            try:
                async with session.get(full_url) as resp:
                    if resp.status != 200:
                        raise TrackerError(
                            f"Tracker at {base_url!r} returned HTTP {resp.status}"
                        )
                    raw_body = await resp.read()
            except aiohttp.ClientError as exc:
                raise TrackerError(f"HTTP error reaching {base_url!r}: {exc}") from exc
            except asyncio.TimeoutError as exc:
                raise TrackerError(f"Timeout reaching tracker {base_url!r}") from exc

        try:
            decoded = BencodeDecoder.decode_data(raw_body)
        except BencodeDecodeError as exc:
            raise TrackerError(
                f"Tracker response from {base_url!r} is not valid bencode: {exc}"
            ) from exc

        if not isinstance(decoded, dict):
            raise TrackerError(
                f"Tracker response from {base_url!r} is not a dict (got {type(decoded).__name__})"
            )

        return TrackerResponse.from_bencode(decoded)