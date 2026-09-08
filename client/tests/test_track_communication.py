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
    UdpTrackerClient,
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

    def test_wss_trackers_filtered_out(self):
        """Only http://, https://, and udp:// URLs should survive — wss:// is skipped."""
        torrent = _make_mock_torrent(
            announce="udp://tracker.example.com:6969",
            announce_list=[
                ["udp://backup.example.com:1337"],
                ["wss://ws.example.com"],
                ["http://http-backup.example.com/announce"],
            ],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls, [
            "udp://tracker.example.com:6969",
            "udp://backup.example.com:1337",
            "http://http-backup.example.com/announce",
        ])

    def test_all_udp_trackers_included(self):
        """When a torrent has UDP trackers, they are included in the list."""
        torrent = _make_mock_torrent(
            announce="udp://tracker.example.com:6969",
            announce_list=[["udp://backup.example.com:1337"]],
        )
        urls = TrackerClient._build_url_list(torrent)
        self.assertEqual(urls, [
            "udp://tracker.example.com:6969",
            "udp://backup.example.com:1337",
        ])

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
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


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



# ---------------------------------------------------------------------------
# UdpTrackerClient — BEP-15 Unit Tests
# ---------------------------------------------------------------------------

class TestUdpTrackerClient(unittest.TestCase):
    """
    Unit tests for UdpTrackerClient (BEP-15) with mocked network sockets.
    """

    def setUp(self):
        self.client = UdpTrackerClient(timeout=2.0)
        self.info_hash = b"\x12" * 20
        self.peer_id = b"-FAST01-123456789012"
        self.url = "udp://tracker.example.com:1337/announce"

    def _setup_mock_socket(self, mock_socket_cls, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [(2, 2, 0, "", ("93.184.216.34", 1337))]
        mock_sock = MagicMock()
        mock_socket_cls.return_value = mock_sock
        return mock_sock

    @patch("socket.getaddrinfo")
    @patch("socket.socket")
    def test_udp_announce_success(self, mock_socket_cls, mock_getaddrinfo):
        mock_sock = self._setup_mock_socket(mock_socket_cls, mock_getaddrinfo)

        sent_packets = []
        def fake_sendto(data, addr):
            sent_packets.append((data, addr))
            return len(data)

        mock_sock.sendto.side_effect = fake_sendto

        connection_id = 0x123456789ABCDEF0
        recv_step = 0

        def fake_recvfrom(bufsize):
            nonlocal recv_step
            recv_step += 1
            if recv_step == 1:
                # Response to connect: action=0, tx_id, connection_id
                connect_req = sent_packets[0][0]
                _, _, connect_tx_id = struct.unpack("!QII", connect_req[:16])
                return struct.pack("!IIQ", 0, connect_tx_id, connection_id), ("93.184.216.34", 1337)
            elif recv_step == 2:
                # Response to announce: action=1, tx_id, interval, leechers, seeders + compact peers
                announce_req = sent_packets[1][0]
                _, _, announce_tx_id = struct.unpack("!QII", announce_req[:16])
                header = struct.pack("!IIIII", 1, announce_tx_id, 1800, 2, 10)
                peer_bytes = _make_compact_peers(("192.168.1.100", 6881), ("10.0.0.1", 51413))
                return header + peer_bytes, ("93.184.216.34", 1337)
            raise AssertionError("Unexpected recvfrom call")

        mock_sock.recvfrom.side_effect = fake_recvfrom

        resp = _run(self.client.announce(
            url=self.url,
            info_hash=self.info_hash,
            peer_id=self.peer_id,
            port=6881,
            uploaded=100,
            downloaded=500,
            left=1000,
            event="started",
            numwant=50,
        ))

        self.assertIsInstance(resp, TrackerResponse)
        self.assertEqual(resp.interval, 1800)
        self.assertEqual(resp.seeders, 10)
        self.assertEqual(resp.leechers, 2)
        self.assertEqual(len(resp.peers), 2)
        self.assertEqual(resp.peers[0].ip, "192.168.1.100")
        self.assertEqual(resp.peers[0].port, 6881)
        self.assertEqual(resp.peers[1].ip, "10.0.0.1")
        self.assertEqual(resp.peers[1].port, 51413)

        # Verify sent packets
        self.assertEqual(len(sent_packets), 2)
        # Connect packet: 16 bytes, magic = 0x41727101980, action = 0
        magic, action, _ = struct.unpack("!QII", sent_packets[0][0])
        self.assertEqual(magic, 0x41727101980)
        self.assertEqual(action, 0)

        # Announce packet: 98 bytes, conn_id, action = 1
        (conn_id_sent, action_sent, _, ih_sent, pid_sent,
         down_sent, left_sent, up_sent, evt_sent, _, _, numwant_sent, port_sent) = struct.unpack(
            "!QII20s20sQQQIIIiH", sent_packets[1][0]
        )
        self.assertEqual(conn_id_sent, connection_id)
        self.assertEqual(action_sent, 1)
        self.assertEqual(ih_sent, self.info_hash)
        self.assertEqual(pid_sent, self.peer_id)
        self.assertEqual(evt_sent, 2)  # 'started' -> 2
        self.assertEqual(numwant_sent, 50)
        self.assertEqual(port_sent, 6881)

    @patch("socket.getaddrinfo")
    @patch("socket.socket")
    def test_udp_connect_error_response(self, mock_socket_cls, mock_getaddrinfo):
        mock_sock = self._setup_mock_socket(mock_socket_cls, mock_getaddrinfo)
        # Action 3 is error
        error_msg = b"Unauthorized IP"
        mock_sock.recvfrom.return_value = (struct.pack("!II", 3, 12345) + error_msg, ("93.184.216.34", 1337))

        with self.assertRaises(TrackerError) as ctx:
            _run(self.client.announce(
                url=self.url,
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=1000,
            ))
        self.assertIn("Unauthorized IP", str(ctx.exception))

    @patch("socket.getaddrinfo")
    @patch("socket.socket")
    def test_udp_announce_error_response(self, mock_socket_cls, mock_getaddrinfo):
        mock_sock = self._setup_mock_socket(mock_socket_cls, mock_getaddrinfo)

        recv_step = 0
        def fake_recvfrom(bufsize):
            nonlocal recv_step
            recv_step += 1
            if recv_step == 1:
                return struct.pack("!IIQ", 0, 12345, 0x1111222233334444), ("93.184.216.34", 1337)
            return struct.pack("!II", 3, 67890) + b"torrent not registered", ("93.184.216.34", 1337)

        # Mock random.randint so connect tx_id matches 12345
        with patch("random.randint", return_value=12345):
            mock_sock.recvfrom.side_effect = fake_recvfrom
            with self.assertRaises(TrackerError) as ctx:
                _run(self.client.announce(
                    url=self.url,
                    info_hash=self.info_hash,
                    peer_id=self.peer_id,
                    port=6881,
                    uploaded=0,
                    downloaded=0,
                    left=1000,
                ))
            self.assertIn("torrent not registered", str(ctx.exception))

    @patch("socket.getaddrinfo")
    @patch("socket.socket")
    def test_udp_connect_tx_id_mismatch(self, mock_socket_cls, mock_getaddrinfo):
        mock_sock = self._setup_mock_socket(mock_socket_cls, mock_getaddrinfo)
        # Return different tx_id
        mock_sock.recvfrom.return_value = (struct.pack("!IIQ", 0, 999999, 0x1234), ("93.184.216.34", 1337))

        with patch("random.randint", return_value=111111):
            with self.assertRaises(TrackerError) as ctx:
                _run(self.client.announce(
                    url=self.url,
                    info_hash=self.info_hash,
                    peer_id=self.peer_id,
                    port=6881,
                    uploaded=0,
                    downloaded=0,
                    left=1000,
                ))
            self.assertIn("transaction ID mismatch", str(ctx.exception))

    @patch("socket.getaddrinfo")
    @patch("socket.socket")
    def test_udp_timeout_raises_tracker_error(self, mock_socket_cls, mock_getaddrinfo):
        import socket
        mock_sock = self._setup_mock_socket(mock_socket_cls, mock_getaddrinfo)
        mock_sock.recvfrom.side_effect = socket.timeout("timed out")

        with self.assertRaises(TrackerError) as ctx:
            _run(self.client.announce(
                url=self.url,
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=1000,
            ))
        self.assertIn("Timeout", str(ctx.exception))

    def test_udp_invalid_url_raises(self):
        with self.assertRaises(TrackerError):
            _run(self.client.announce(
                url="udp://",
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=1000,
            ))

    @patch("socket.getaddrinfo")
    def test_udp_dns_resolution_failure(self, mock_getaddrinfo):
        import socket
        mock_getaddrinfo.side_effect = socket.gaierror("Name or service not known")

        with self.assertRaises(TrackerError) as ctx:
            _run(self.client.announce(
                url=self.url,
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=6881,
                uploaded=0,
                downloaded=0,
                left=1000,
            ))
        self.assertIn("DNS resolution failed", str(ctx.exception))

    def test_tracker_client_routes_udp_url(self):
        """TrackerClient.announce() delegates to UdpTrackerClient when URL is udp://"""
        torrent = _make_mock_torrent(announce="udp://tracker.example.com:1337/announce")
        expected_resp = TrackerResponse(interval=1800, peers=[PeerInfo("1.2.3.4", 6881)])

        client = TrackerClient()
        client._udp_client = MagicMock()
        client._udp_client.announce = AsyncMock(return_value=expected_resp)

        result = _run(client.announce(
            torrent=torrent,
            peer_id=self.peer_id,
            port=6881,
            uploaded=0,
            downloaded=0,
            left=1000,
        ))

        self.assertEqual(result, expected_resp)
        client._udp_client.announce.assert_awaited_once_with(
            url="udp://tracker.example.com:1337/announce",
            info_hash=torrent.info_hash,
            peer_id=self.peer_id,
            port=6881,
            uploaded=0,
            downloaded=0,
            left=1000,
            event=None,
            numwant=50,
        )


if __name__ == "__main__":
    unittest.main()

