import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.bencode import encode
from modules.torrentFileParser import FileInfo, TorrentFile, TorrentParseError


# ---------------------------------------------------------------------------
# Helpers: build valid bencoded .torrent payloads programmatically
# ---------------------------------------------------------------------------

def _make_pieces(n: int = 1) -> bytes:
    """Return n * 20 bytes of fake piece hashes."""
    return bytes(range(20)) * n


def _make_single_file_torrent(
    *,
    name: bytes = b"example.txt",
    length: int = 1024,
    piece_length: int = 512,
    num_pieces: int = 2,
    announce: bytes = b"http://tracker.example.com/announce",
    extra_info: dict | None = None,
    extra_top: dict | None = None,
) -> bytes:
    info: dict = {
        b"name": name,
        b"length": length,
        b"piece length": piece_length,
        b"pieces": _make_pieces(num_pieces),
    }
    if extra_info:
        info.update(extra_info)
    meta: dict = {
        b"announce": announce,
        b"info": info,
    }
    if extra_top:
        meta.update(extra_top)
    return encode(meta)


def _make_multi_file_torrent(
    *,
    name: bytes = b"MyAlbum",
    files: list[dict] | None = None,
    piece_length: int = 512,
    num_pieces: int = 3,
    announce: bytes = b"http://tracker.example.com/announce",
    extra_top: dict | None = None,
) -> bytes:
    if files is None:
        files = [
            {b"path": [b"track01.mp3"], b"length": 3000000},
            {b"path": [b"track02.mp3"], b"length": 4000000},
            {b"path": [b"art", b"cover.jpg"], b"length": 50000},
        ]
    info: dict = {
        b"name": name,
        b"files": files,
        b"piece length": piece_length,
        b"pieces": _make_pieces(num_pieces),
    }
    meta: dict = {
        b"announce": announce,
        b"info": info,
    }
    if extra_top:
        meta.update(extra_top)
    return encode(meta)


def _info_hash_from_raw(raw: bytes) -> bytes:
    """Independently compute the expected info_hash from raw bencode bytes."""
    marker = b"4:info"
    start = raw.find(marker) + len(marker)
    # Walk to find end of info value
    from modules.bencode import BencodeDecoder
    dec = BencodeDecoder(raw[start:])
    dec._parse_next()
    return hashlib.sha1(raw[start : start + dec._index]).digest()


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestSingleFileTorrent(unittest.TestCase):

    def setUp(self):
        self.raw = _make_single_file_torrent()
        self.torrent = TorrentFile.from_bytes(self.raw)

    def test_announce(self):
        self.assertEqual(self.torrent.announce, "http://tracker.example.com/announce")

    def test_name(self):
        self.assertEqual(self.torrent.name, b"example.txt")

    def test_piece_length(self):
        self.assertEqual(self.torrent.piece_length, 512)

    def test_num_pieces(self):
        self.assertEqual(self.torrent.num_pieces, 2)

    def test_piece_hashes_are_20_bytes_each(self):
        for h in self.torrent.piece_hashes:
            self.assertEqual(len(h), 20)

    def test_single_file_structure(self):
        self.assertFalse(self.torrent.is_multi_file)
        self.assertEqual(len(self.torrent.files), 1)

    def test_file_info(self):
        f: FileInfo = self.torrent.files[0]
        self.assertEqual(f.path, "example.txt")
        self.assertEqual(f.length, 1024)

    def test_total_length(self):
        self.assertEqual(self.torrent.total_length, 1024)

    def test_info_hash_is_20_bytes(self):
        self.assertEqual(len(self.torrent.info_hash), 20)
        self.assertIsInstance(self.torrent.info_hash, bytes)

    def test_info_hash_matches_expected(self):
        expected = _info_hash_from_raw(self.raw)
        self.assertEqual(self.torrent.info_hash, expected)

    def test_calculate_info_hash_returns_cached(self):
        self.assertEqual(self.torrent.calculate_info_hash(), self.torrent.info_hash)

    def test_default_optional_fields(self):
        self.assertIsNone(self.torrent.comment)
        self.assertIsNone(self.torrent.created_by)
        self.assertIsNone(self.torrent.creation_date)
        self.assertFalse(self.torrent.is_private)
        self.assertEqual(self.torrent.announce_list, [])


class TestMultiFileTorrent(unittest.TestCase):

    def setUp(self):
        self.raw = _make_multi_file_torrent()
        self.torrent = TorrentFile.from_bytes(self.raw)

    def test_is_multi_file(self):
        self.assertTrue(self.torrent.is_multi_file)

    def test_file_count(self):
        self.assertEqual(len(self.torrent.files), 3)

    def test_file_paths_rooted_at_torrent_name(self):
        paths = [f.path for f in self.torrent.files]
        self.assertIn("MyAlbum/track01.mp3", paths)
        self.assertIn("MyAlbum/track02.mp3", paths)
        self.assertIn("MyAlbum/art/cover.jpg", paths)

    def test_file_lengths(self):
        lengths = {f.path.split("/")[-1]: f.length for f in self.torrent.files}
        self.assertEqual(lengths["track01.mp3"], 3000000)
        self.assertEqual(lengths["track02.mp3"], 4000000)
        self.assertEqual(lengths["cover.jpg"], 50000)

    def test_total_length(self):
        self.assertEqual(self.torrent.total_length, 3000000 + 4000000 + 50000)

    def test_info_hash_matches_expected(self):
        expected = _info_hash_from_raw(self.raw)
        self.assertEqual(self.torrent.info_hash, expected)

    def test_announce(self):
        self.assertEqual(self.torrent.announce, "http://tracker.example.com/announce")


class TestOptionalFields(unittest.TestCase):

    def _parse_with_extras(self, extra_top: dict, extra_info: dict | None = None) -> TorrentFile:
        raw = _make_single_file_torrent(extra_top=extra_top, extra_info=extra_info)
        return TorrentFile.from_bytes(raw)

    def test_comment(self):
        torrent = self._parse_with_extras({b"comment": b"My test comment"})
        self.assertEqual(torrent.comment, "My test comment")

    def test_created_by(self):
        torrent = self._parse_with_extras({b"created by": b"uTorrent/3.5.5"})
        self.assertEqual(torrent.created_by, "uTorrent/3.5.5")

    def test_creation_date(self):
        torrent = self._parse_with_extras({b"creation date": 1700000000})
        self.assertEqual(torrent.creation_date, 1700000000)

    def test_private_flag(self):
        torrent = self._parse_with_extras({}, extra_info={b"private": 1})
        self.assertTrue(torrent.is_private)

    def test_announce_list_bep12(self):
        announce_list = [
            [b"http://tracker1.example.com/announce"],
            [b"http://tracker2.example.com/announce", b"http://tracker3.example.com/announce"],
        ]
        torrent = self._parse_with_extras({b"announce-list": announce_list})
        self.assertEqual(len(torrent.announce_list), 2)
        self.assertIn("http://tracker1.example.com/announce", torrent.announce_list[0])
        self.assertIn("http://tracker2.example.com/announce", torrent.announce_list[1])
        self.assertIn("http://tracker3.example.com/announce", torrent.announce_list[1])


class TestInfoHashStability(unittest.TestCase):
    """The info_hash must be identical across multiple parses of the same file."""

    def test_same_raw_same_hash(self):
        raw = _make_single_file_torrent()
        t1 = TorrentFile.from_bytes(raw)
        t2 = TorrentFile.from_bytes(raw)
        self.assertEqual(t1.info_hash, t2.info_hash)

    def test_different_metadata_different_hash(self):
        raw1 = _make_single_file_torrent(name=b"file_a.txt")
        raw2 = _make_single_file_torrent(name=b"file_b.txt")
        t1 = TorrentFile.from_bytes(raw1)
        t2 = TorrentFile.from_bytes(raw2)
        self.assertNotEqual(t1.info_hash, t2.info_hash)

    def test_changing_only_announce_doesnt_change_hash(self):
        """info_hash only covers the info dict, not top-level announce."""
        raw1 = _make_single_file_torrent(announce=b"http://tracker1.example.com/announce")
        raw2 = _make_single_file_torrent(announce=b"http://tracker2.example.com/announce")
        t1 = TorrentFile.from_bytes(raw1)
        t2 = TorrentFile.from_bytes(raw2)
        self.assertEqual(t1.info_hash, t2.info_hash)


class TestFromPath(unittest.TestCase):
    """Tests for the from_path() factory."""

    def test_from_path_reads_file(self):
        raw = _make_single_file_torrent()
        with tempfile.NamedTemporaryFile(suffix=".torrent", delete=False) as f:
            f.write(raw)
            tmp_path = Path(f.name)
        try:
            torrent = TorrentFile.from_path(tmp_path)
            self.assertEqual(torrent.announce, "http://tracker.example.com/announce")
            self.assertEqual(torrent.info_hash, _info_hash_from_raw(raw))
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_from_path_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            TorrentFile.from_path("/nonexistent/path/fake.torrent")


class TestErrorHandling(unittest.TestCase):

    def test_non_bencode_bytes_raises(self):
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(b"this is not bencode at all!!!")

    def test_missing_announce_raises(self):
        raw = encode({b"info": {
            b"name": b"x.txt",
            b"length": 100,
            b"piece length": 512,
            b"pieces": _make_pieces(1),
        }})
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)

    def test_missing_info_raises(self):
        raw = encode({b"announce": b"http://t.example.com/announce"})
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)

    def test_missing_pieces_raises(self):
        raw = encode({
            b"announce": b"http://t.example.com/announce",
            b"info": {
                b"name": b"x.txt",
                b"length": 100,
                b"piece length": 512,
                # "pieces" key omitted
            },
        })
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)

    def test_pieces_not_multiple_of_20_raises(self):
        raw = encode({
            b"announce": b"http://t.example.com/announce",
            b"info": {
                b"name": b"x.txt",
                b"length": 100,
                b"piece length": 512,
                b"pieces": b"notenoughbytes",  # 14 bytes, not multiple of 20
            },
        })
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)

    def test_missing_length_single_file_raises(self):
        raw = encode({
            b"announce": b"http://t.example.com/announce",
            b"info": {
                b"name": b"x.txt",
                b"piece length": 512,
                b"pieces": _make_pieces(1),
                # "length" omitted, no "files" list either
            },
        })
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)

    def test_top_level_not_dict_raises(self):
        # A valid bencode integer instead of a dict
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(b"i42e")

    def test_invalid_piece_length_zero_raises(self):
        raw = encode({
            b"announce": b"http://t.example.com/announce",
            b"info": {
                b"name": b"x.txt",
                b"length": 100,
                b"piece length": 0,
                b"pieces": _make_pieces(1),
            },
        })
        with self.assertRaises(TorrentParseError):
            TorrentFile.from_bytes(raw)


class TestRepr(unittest.TestCase):

    def test_repr_contains_name_and_hash(self):
        raw = _make_single_file_torrent(name=b"mymovie.mkv")
        torrent = TorrentFile.from_bytes(raw)
        r = repr(torrent)
        self.assertIn("mymovie.mkv", r)
        self.assertIn("info_hash", r)
        self.assertIn(torrent.info_hash.hex(), r)


if __name__ == "__main__":
    unittest.main()
