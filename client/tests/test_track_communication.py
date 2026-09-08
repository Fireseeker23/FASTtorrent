import asyncio
import ipaddress
import struct
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.bencode import encode
from modules.trackCommunication import (
    PeerInfo,
    TrackerClient,
    TrackerError,
    TrackerResponse,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compact_peer(ip: str, port: int) -> bytes:
    """Encode one peer into 6-byte compact format."""
    ip_int = int(ipaddress.IPv4Address(ip))
    return struct.pack("!IH", ip_int, port)


def _make_compact_peers(*peers: tuple[str, int]) -> bytes:
    """Build compact peers blob from (ip, port) tuples."""
    return b"".join(_compact_peer(ip, port) for ip, port in peers)


def _make_tracker_response(
    *,
    peers: bytes | list,
    interval: int = 1800,
    min_interval: int | None = None,
    seeders: int | None = None,
    leechers: int | None = None,
    tracker_id: bytes | None = None,
    warning: bytes | None = None,
) -> bytes:
    """Return a bencoded tracker response ready to be 'received' from a tracker."""
    d = {
        b"interval": interval,
        b"peers": peers,
    }
    if min_interval is not None:
        d[b"min interval"] = min_interval
    if seeders is not None:
        d[b"complete"] = seeders
    if leechers is not None:
        d[b"incomplete"] = leechers
    if tracker_id is not None:
        d[b"tracker id"] = tracker_id
    if warning is not None:
        d[b"warning message"] = warning
    return encode(d)


def _make_failure_response(reason: str) -> bytes:
    return encode({b"failure reason": reason.encode()})


def _make_mock_torrent(
    announce: str = "http://tracker.example.com/announce",
    announce_list: list | None = None,
    info_hash: bytes = b"\xab" * 20,
    total_length: int = 1024 * 1024,
) -> MagicMock:
    """Build a lightweight mock TorrentFile."""
    t = MagicMock()
    t.announce = announce
    t.announce_list = announce_list or []
    t.info_hash = info_hash
    t.total_length = total_length
    return t


# ---------------------------------------------------------------------------
# TrackerResponse.from_bencode
# ---------------------------------------------------------------------------

class TestTrackerResponseFromBencode(unittest.TestCase):

    def _decode(self, raw: bytes) -> dict:
        from modules.bencode import decode
        return decode(raw)

    def test_compact_peers_parsed(self):
        compact = _make_compact_peers(("1.2.3.4", 6881), ("5.6.7.8", 51413))
        raw = _make_tracker_response(peers=compact, interval=1800)
        resp = TrackerResponse.from_bencode(self._decode(raw))

        self.assertEqual(resp.interval, 1800)
        self.assertEqual(len(resp.peers), 2)
        self.assertEqual(resp.peers[0].ip, "1.2.3.4")
        self.assertEqual(resp.peers[0].port, 6881)
        self.assertEqual(resp.peers[1].ip, "5.6.7.8")
        self.assertEqual(resp.peers[1].port, 51413)

    def test_dict_peers_parsed(self):
        dict_peers = [
            {b"ip": b"10.0.0.1", b"port": 6881},
            {b"ip": b"10.0.0.2", b"port": 6882},
        ]
        raw = _make_tracker_response(peers=dict_peers, interval=900)
        resp = TrackerResponse.from_bencode(self._decode(raw))

        self.assertEqual(resp.interval, 900)
        self.assertEqual(len(resp.peers), 2)
        self.assertEqual(resp.peers[0].ip, "10.0.0.1")
        self.assertEqual(resp.peers[1].port, 6882)

    def test_optional_fields_populated(self):
        compact = _make_compact_peers(("1.1.1.1", 80))
        raw = _make_tracker_response(
            peers=compact,
            interval=1800,
            min_interval=60,
            seeders=10,
            leechers=3,
            tracker_id=b"mytracker",
            warning=b"low disk space",
        )
        resp = TrackerResponse.from_bencode(self._decode(raw))

        self.assertEqual(resp.min_interval, 60)
        self.assertEqual(resp.seeders, 10)
        self.assertEqual(resp.leechers, 3)
        self.assertEqual(resp.tracker_id, b"mytracker")
        self.assertEqual(resp.warning_message, "low disk space")

    def test_empty_peers_list(self):
        raw = _make_tracker_response(peers=b"", interval=1800)
        resp = TrackerResponse.from_bencode(self._decode(raw))
        self.assertEqual(resp.peers, [])

    def test_failure_reason_raises(self):
        raw = _make_failure_response("Your client is banned")
        with self.assertRaises(TrackerError) as ctx:
            TrackerResponse.from_bencode(self._decode(raw))
        self.assertIn("Your client is banned", str(ctx.exception))

    def test_missing_interval_raises(self):
        raw = encode({b"peers": b""})   # no interval
        with self.assertRaises(TrackerError):
            TrackerResponse.from_bencode(self._decode(raw))

    def test_missing_peers_raises(self):
        raw = encode({b"interval": 1800})   # no peers
        with self.assertRaises(TrackerError):
            TrackerResponse.from_bencode(self._decode(raw))

    def test_optional_fields_default_to_none(self):
        compact = _make_compact_peers(("2.2.2.2", 1234))
        raw = _make_tracker_response(peers=compact, interval=600)
        resp = TrackerResponse.from_bencode(self._decode(raw))

        self.assertIsNone(resp.min_interval)
        self.assertIsNone(resp.seeders)
        self.assertIsNone(resp.leechers)
        self.assertIsNone(resp.tracker_id)
        self.assertIsNone(resp.warning_message)


# ---------------------------------------------------------------------------
# Compact / dict peer parsing
# ---------------------------------------------------------------------------

class TestCompactPeerParsing(unittest.TestCase):

    def test_single_peer(self):
        data = _compact_peer("192.168.1.1", 6881)
        peers = TrackerResponse._parse_compact_peers(data)
        self.assertEqual(len(peers), 1)
        self.assertEqual(peers[0].ip, "192.168.1.1")
        self.assertEqual(peers[0].port, 6881)

    def test_multiple_peers(self):
        data = _make_compact_peers(
            ("10.0.0.1", 6881),
            ("10.0.0.2", 6882),
            ("10.0.0.3", 6883),
        )
        peers = TrackerResponse._parse_compact_peers(data)
        self.assertEqual(len(peers), 3)
        ips = [p.ip for p in peers]
        self.assertIn("10.0.0.1", ips)
        self.assertIn("10.0.0.3", ips)

    def test_boundary_ip_addresses(self):
        data = _make_compact_peers(("0.0.0.0", 0), ("255.255.255.255", 65535))
        peers = TrackerResponse._parse_compact_peers(data)
        self.assertEqual(peers[0].ip, "0.0.0.0")
        self.assertEqual(peers[0].port, 0)
        self.assertEqual(peers[1].ip, "255.255.255.255")
        self.assertEqual(peers[1].port, 65535)

    def test_non_multiple_of_6_raises(self):
        with self.assertRaises(TrackerError):
            TrackerResponse._parse_compact_peers(b"\x01\x02\x03")  # 3 bytes

    def test_empty_bytes_returns_empty_list(self):
        self.assertEqual(TrackerResponse._parse_compact_peers(b""), [])


class TestDictPeerParsing(unittest.TestCase):

    def test_bytes_keys(self):
        peers = TrackerResponse._parse_dict_peers([
            {b"ip": b"1.2.3.4", b"port": 6881},
        ])
        self.assertEqual(peers[0].ip, "1.2.3.4")
        self.assertEqual(peers[0].port, 6881)

    def test_str_keys(self):
        peers = TrackerResponse._parse_dict_peers([
            {"ip": "5.6.7.8", "port": 1234},
        ])
        self.assertEqual(peers[0].ip, "5.6.7.8")

    def test_missing_ip_raises(self):
        with self.assertRaises(TrackerError):
            TrackerResponse._parse_dict_peers([{b"port": 6881}])

    def test_missing_port_raises(self):
        with self.assertRaises(TrackerError):
            TrackerResponse._parse_dict_peers([{b"ip": b"1.2.3.4"}])

    def test_non_dict_entry_raises(self):
        with self.assertRaises(TrackerError):
            TrackerResponse._parse_dict_peers(["not-a-dict"])

    def test_unknown_peers_type_raises(self):
        with self.assertRaises(TrackerError):
            TrackerResponse._parse_peers(12345)


# ---------------------------------------------------------------------------
# TrackerClient._build_url_list
# ---------------------------------------------------------------------------

class TestBuildUrlList(unittest.TestCase):

    def test_primary_announce_only(self):
        torrent = _make_mock_torrent(announce="http://t1.example.com/announce")
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls, ["http://t1.example.com/announce"])

    def test_announce_list_appended(self):
        torrent = _make_mock_torrent(
            announce="http://primary.example.com/announce",
            announce_list=[
                ["http://backup1.example.com/announce"],
                ["http://backup2.example.com/announce"],
            ],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls[0], "http://primary.example.com/announce")
        self.assertIn("http://backup1.example.com/announce", urls)
        self.assertIn("http://backup2.example.com/announce", urls)
        self.assertEqual(len(urls), 3)

    def test_udp_and_wss_trackers_filtered_out(self):
        """Only http:// and https:// URLs should survive — udp:// and wss:// are skipped."""
        torrent = _make_mock_torrent(
            announce="udp://tracker.example.com:6969",
            announce_list=[
                ["udp://backup.example.com:1337"],
                ["wss://ws.example.com"],
                ["http://http-backup.example.com/announce"],
            ],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls, ["http://http-backup.example.com/announce"])

    def test_all_udp_returns_empty_list(self):
        """When a torrent has only UDP trackers, the list is empty."""
        torrent = _make_mock_torrent(
            announce="udp://tracker.example.com:6969",
            announce_list=[["udp://backup.example.com:1337"]],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls, [])

    def test_duplicates_removed(self):
        torrent = _make_mock_torrent(
            announce="http://t.example.com/announce",
            announce_list=[["http://t.example.com/announce"]],  # same as primary
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(len(urls), 1)

    def test_primary_always_first(self):
        torrent = _make_mock_torrent(
            announce="http://primary.example.com/announce",
            announce_list=[["http://backup.example.com/announce"]],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls[0], "http://primary.example.com/announce")


# ---------------------------------------------------------------------------
# TrackerClient._build_params
# ---------------------------------------------------------------------------

class TestBuildParams(unittest.TestCase):

    def _params(self, event=None):
        return TrackerClient._build_params(
            info_hash=b"\xab" * 20,
            peer_id=b"-AG0001-" + b"x" * 12,
            port=6881,
            uploaded=0,
            downloaded=512,
            left=1024,
            event=event,
            compact=1,
            numwant=50,
        )

    def test_required_keys_present(self):
        p = self._params()
        for key in ("info_hash", "peer_id", "port", "uploaded", "downloaded", "left", "compact", "numwant"):
            self.assertIn(key, p)

    def test_event_included_when_set(self):
        p = self._params(event="started")
        self.assertEqual(p["event"], "started")

    def test_event_omitted_when_none(self):
        p = self._params(event=None)
        self.assertNotIn("event", p)

    def test_info_hash_is_percent_encoded_string(self):
        p = self._params()
        # Should be a percent-encoded string, not raw bytes
        self.assertIsInstance(p["info_hash"], str)
        self.assertIn("%", p["info_hash"])  # raw bytes always produce %XX sequences

    def test_peer_id_is_percent_encoded_string(self):
        p = self._params()
        self.assertIsInstance(p["peer_id"], str)

    def test_raw_bytes_stored_in_private_keys(self):
        p = self._params()
        self.assertEqual(p["_info_hash_raw"], b"\xab" * 20)
        self.assertIsInstance(p["_peer_id_raw"], bytes)


# ---------------------------------------------------------------------------
# TrackerClient.announce — mocked HTTP layer
# ---------------------------------------------------------------------------

def _run(coro):
    """Run an async coroutine in a new event loop (for unittest compatibility)."""
    return asyncio.get_event_loop().run_until_complete(coro)


class TestTrackerClientAnnounce(unittest.TestCase):
    """
    Tests for TrackerClient.announce() using mocked aiohttp sessions so
    no real network calls are made.
    """

    PEER_ID = b"-AG0001-" + b"x" * 12

    def _make_response_mock(self, body: bytes, status: int = 200) -> MagicMock:
        """Build a mock aiohttp response context manager."""
        resp = AsyncMock()
        resp.status = status
        resp.read = AsyncMock(return_value=body)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        return resp

    def _make_session_mock(self, response_mock) -> MagicMock:
        """Build a mock aiohttp.ClientSession context manager."""
        session = MagicMock()
        session.get = MagicMock(return_value=response_mock)
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        return session

    def test_successful_announce_compact(self):
        compact = _make_compact_peers(("1.2.3.4", 6881))
        body = _make_tracker_response(peers=compact, interval=1800)

        resp_mock = self._make_response_mock(body)
        session_mock = self._make_session_mock(resp_mock)

        torrent = _make_mock_torrent()
        client = TrackerClient()

        with patch("modules.trackCommunication.aiohttp.ClientSession", return_value=session_mock):
            result = _run(client.announce(
                torrent=torrent,
                peer_id=self.PEER_ID,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=torrent.total_length,
                event="started",
            ))

        self.assertIsInstance(result, TrackerResponse)
        self.assertEqual(result.interval, 1800)
        self.assertEqual(len(result.peers), 1)
        self.assertEqual(result.peers[0].ip, "1.2.3.4")
        self.assertEqual(result.peers[0].port, 6881)

    def test_http_error_falls_back_to_next_tracker(self):
        """Primary tracker returns HTTP 500; backup tracker succeeds."""
        compact = _make_compact_peers(("9.9.9.9", 9999))
        good_body = _make_tracker_response(peers=compact, interval=600)

        bad_resp = self._make_response_mock(b"", status=500)
        good_resp = self._make_response_mock(good_body, status=200)

        call_count = 0

        def get_side_effect(url, **kwargs):
            nonlocal call_count
            call_count += 1
            return bad_resp if call_count == 1 else good_resp

        session = MagicMock()
        session.get = MagicMock(side_effect=get_side_effect)
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)

        torrent = _make_mock_torrent(
            announce="http://primary.example.com/announce",
            announce_list=[["http://backup.example.com/announce"]],
        )
        client = TrackerClient()

        with patch("modules.trackCommunication.aiohttp.ClientSession", return_value=session):
            result = _run(client.announce(
                torrent=torrent,
                peer_id=self.PEER_ID,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=1024,
            ))

        self.assertEqual(result.peers[0].ip, "9.9.9.9")

    def test_tracker_failure_reason_raises(self):
        body = _make_failure_response("info_hash not found")
        resp_mock = self._make_response_mock(body)
        session_mock = self._make_session_mock(resp_mock)

        torrent = _make_mock_torrent()
        client = TrackerClient()

        with patch("modules.trackCommunication.aiohttp.ClientSession", return_value=session_mock):
            with self.assertRaises(TrackerError) as ctx:
                _run(client.announce(
                    torrent=torrent,
                    peer_id=self.PEER_ID,
                    port=6881,
                    uploaded=0,
                    downloaded=0,
                    left=1024,
                    event="started",
                ))
        self.assertIn("info_hash not found", str(ctx.exception))

    def test_all_trackers_fail_raises(self):
        """If every tracker returns an error, TrackerError is raised."""
        bad_resp = self._make_response_mock(b"", status=503)
        session = MagicMock()
        session.get = MagicMock(return_value=bad_resp)
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)

        torrent = _make_mock_torrent(announce="http://down.example.com/announce")
        client = TrackerClient()

        with patch("modules.trackCommunication.aiohttp.ClientSession", return_value=session):
            with self.assertRaises(TrackerError):
                _run(client.announce(
                    torrent=torrent,
                    peer_id=self.PEER_ID,
                    port=6881,
                    uploaded=0,
                    downloaded=0,
                    left=1024,
                ))

    def test_non_bencode_response_raises(self):
        resp_mock = self._make_response_mock(b"this is not bencode!!!")
        session_mock = self._make_session_mock(resp_mock)

        torrent = _make_mock_torrent()
        client = TrackerClient()

        with patch("modules.trackCommunication.aiohttp.ClientSession", return_value=session_mock):
            with self.assertRaises(TrackerError):
                _run(client.announce(
                    torrent=torrent,
                    peer_id=self.PEER_ID,
                    port=6881,
                    uploaded=0,
                    downloaded=0,
                    left=1024,
                ))


if __name__ == "__main__":
    unittest.main()
