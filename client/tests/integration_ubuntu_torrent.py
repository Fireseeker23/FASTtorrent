"""
integration_ubuntu_torrent.py — Real-file integration test.

Uses the actual ubuntu-26.04.1-desktop-amd64.iso.torrent that lives
in the client/ directory to exercise the full pipeline:

  1. bencode.py      — raw binary → Python dict
  2. torrentFileParser.py — Python dict → TorrentFile object
  3. trackCommunication.py — TorrentFile → live HTTP tracker announce → peers

Run with:
    uv run --with pytest python -m pytest tests/integration_ubuntu_torrent.py -v -s
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TORRENT_PATH = Path(__file__).resolve().parent.parent / "ubuntu-26.04.1-desktop-amd64.iso.torrent"

# Expected constants from the real file (verified manually)
EXPECTED_NAME        = b"ubuntu-26.04.1-desktop-amd64.iso"
EXPECTED_INFO_HASH   = "5b1e0d988fc7a0c9e99bd852071681a59974b39f"
EXPECTED_PIECE_LEN   = 262144          # 256 KB
EXPECTED_NUM_PIECES  = 24729
EXPECTED_TOTAL_LEN   = 6482409472      # ~6.04 GB
EXPECTED_ANNOUNCE    = "https://torrent.ubuntu.com/announce"
EXPECTED_COMMENT     = "Ubuntu CD releases.ubuntu.com"
EXPECTED_CREATED_BY  = "mktorrent 1.1"

PEER_ID = b"-AG0001-" + b"x" * 12     # 20-byte fake client ID
PORT    = 6881


# ---------------------------------------------------------------------------
# Stage 1 — bencode.py: raw bytes → Python dict
# ---------------------------------------------------------------------------

class TestStage1BencodeParsing(unittest.TestCase):
    """Verify bencode.py can decode the real torrent file without errors."""

    @classmethod
    def setUpClass(cls):
        from modules.bencode import decode
        cls.raw  = TORRENT_PATH.read_bytes()
        cls.meta = decode(cls.raw)

    def test_file_exists(self):
        self.assertTrue(TORRENT_PATH.exists(), f"Torrent not found: {TORRENT_PATH}")

    def test_decode_produces_dict(self):
        self.assertIsInstance(self.meta, dict)

    def test_top_level_keys_present(self):
        keys = {k.decode() if isinstance(k, bytes) else k for k in self.meta}
        self.assertIn("announce", keys)
        self.assertIn("info", keys)

    def test_info_is_dict(self):
        info = self.meta.get(b"info") or self.meta.get("info")
        self.assertIsInstance(info, dict)

    def test_pieces_field_is_bytes(self):
        info   = self.meta.get(b"info") or self.meta.get("info")
        pieces = info.get(b"pieces") or info.get("pieces")
        self.assertIsInstance(pieces, bytes)

    def test_pieces_length_divisible_by_20(self):
        info   = self.meta.get(b"info") or self.meta.get("info")
        pieces = info.get(b"pieces") or info.get("pieces")
        self.assertEqual(len(pieces) % 20, 0)

    def test_raw_file_size_is_nonzero(self):
        self.assertGreater(len(self.raw), 0)


# ---------------------------------------------------------------------------
# Stage 2 — torrentFileParser.py: Python dict → TorrentFile
# ---------------------------------------------------------------------------

class TestStage2TorrentFileParsing(unittest.TestCase):
    """Verify torrentFileParser produces correct fields from the real file."""

    @classmethod
    def setUpClass(cls):
        from modules.torrentFileParser import TorrentFile
        cls.torrent = TorrentFile.from_path(TORRENT_PATH)

    def test_name(self):
        self.assertEqual(self.torrent.name, EXPECTED_NAME)

    def test_announce_url(self):
        self.assertEqual(self.torrent.announce, EXPECTED_ANNOUNCE)

    def test_announce_list_has_backup_tracker(self):
        all_urls = [url for tier in self.torrent.announce_list for url in tier]
        self.assertIn("https://ipv6.torrent.ubuntu.com/announce", all_urls)

    def test_info_hash_hex(self):
        self.assertEqual(self.torrent.info_hash.hex(), EXPECTED_INFO_HASH)

    def test_info_hash_length(self):
        self.assertEqual(len(self.torrent.info_hash), 20)

    def test_piece_length(self):
        self.assertEqual(self.torrent.piece_length, EXPECTED_PIECE_LEN)

    def test_num_pieces(self):
        self.assertEqual(self.torrent.num_pieces, EXPECTED_NUM_PIECES)

    def test_each_piece_hash_is_20_bytes(self):
        for h in self.torrent.piece_hashes:
            self.assertEqual(len(h), 20)

    def test_total_length(self):
        self.assertEqual(self.torrent.total_length, EXPECTED_TOTAL_LEN)

    def test_is_single_file(self):
        self.assertFalse(self.torrent.is_multi_file)
        self.assertEqual(len(self.torrent.files), 1)

    def test_file_path(self):
        self.assertEqual(self.torrent.files[0].path, "ubuntu-26.04.1-desktop-amd64.iso")

    def test_file_length(self):
        self.assertEqual(self.torrent.files[0].length, EXPECTED_TOTAL_LEN)

    def test_comment(self):
        self.assertEqual(self.torrent.comment, EXPECTED_COMMENT)

    def test_created_by(self):
        self.assertEqual(self.torrent.created_by, EXPECTED_CREATED_BY)

    def test_is_not_private(self):
        self.assertFalse(self.torrent.is_private)

    def test_repr_contains_name_and_hash(self):
        r = repr(self.torrent)
        self.assertIn("ubuntu-26.04.1-desktop-amd64.iso", r)
        self.assertIn(EXPECTED_INFO_HASH, r)


# ---------------------------------------------------------------------------
# Stage 3 — trackCommunication.py: live HTTP tracker announce
# ---------------------------------------------------------------------------

class TestStage3TrackerAnnounce(unittest.TestCase):
    """
    Send a real announce to Ubuntu's tracker and verify we get peers back.

    This test makes a live network request.
    Skip it if you are offline by setting the env var:
        SKIP_NETWORK_TESTS=1
    """

    @classmethod
    def setUpClass(cls):
        import os
        cls.skip = os.environ.get("SKIP_NETWORK_TESTS", "0") == "1"
        if cls.skip:
            return

        from modules.torrentFileParser import TorrentFile
        from modules.trackCommunication import TrackerClient
        cls.torrent = TorrentFile.from_path(TORRENT_PATH)
        cls.client  = TrackerClient()

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _announce(self):
        return self._run(self.client.announce(
            torrent=self.torrent,
            peer_id=PEER_ID,
            port=PORT,
            uploaded=0,
            downloaded=0,
            left=self.torrent.total_length,
            event="started",
            numwant=30,
        ))

    def test_tracker_returns_response(self):
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")

        from modules.trackCommunication import TrackerResponse
        resp = self._announce()
        self.assertIsInstance(resp, TrackerResponse)

    def test_interval_is_positive(self):
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")
        resp = self._announce()
        self.assertGreater(resp.interval, 0)

    def test_peers_returned(self):
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")
        from modules.trackCommunication import PeerInfo
        resp = self._announce()
        self.assertIsInstance(resp.peers, list)
        self.assertGreater(len(resp.peers), 0, "Tracker returned zero peers")

    def test_peers_have_valid_ip_and_port(self):
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")
        import ipaddress
        resp = self._announce()
        for peer in resp.peers:
            try:
                ipaddress.ip_address(peer.ip)   # accepts both IPv4 and IPv6
            except ValueError:
                self.fail(f"Invalid IP address: {peer.ip!r}")
            self.assertTrue(1 <= peer.port <= 65535, f"Invalid port: {peer.port}")


    def test_seeders_and_leechers_are_non_negative(self):
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")
        resp = self._announce()
        if resp.seeders is not None:
            self.assertGreaterEqual(resp.seeders, 0)
        if resp.leechers is not None:
            self.assertGreaterEqual(resp.leechers, 0)

    def test_print_summary(self):
        """Prints a human-readable summary — not an assertion, just for -s output."""
        if self.skip:
            self.skipTest("SKIP_NETWORK_TESTS=1")
        from modules.torrentFileParser import TorrentFile
        t = self.torrent
        resp = self._announce()

        print("\n" + "=" * 60)
        print("  Ubuntu Torrent Integration Summary")
        print("=" * 60)
        print(f"  Name       : {t.name.decode()}")
        print(f"  Info hash  : {t.info_hash.hex()}")
        print(f"  Size       : {t.total_length / 1024**3:.2f} GB")
        print(f"  Pieces     : {t.num_pieces:,}  ({t.piece_length // 1024} KB each)")
        print(f"  Tracker    : {t.announce}")
        print(f"  Interval   : {resp.interval}s")
        print(f"  Seeders    : {resp.seeders}")
        print(f"  Leechers   : {resp.leechers}")
        print(f"  Peers got  : {len(resp.peers)}")
        print("-" * 60)
        for peer in resp.peers[:10]:
            print(f"    {peer}")
        if len(resp.peers) > 10:
            print(f"    ... and {len(resp.peers) - 10} more")
        print("=" * 60)


if __name__ == "__main__":
    unittest.main(verbosity=2)
