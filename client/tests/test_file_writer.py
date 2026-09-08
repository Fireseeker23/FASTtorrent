"""
test_file_writer.py — Comprehensive unit tests for FileWriter.

Tests single-file and multi-file torrent layouts, out-of-order piece writing,
pieces spanning multiple files, edge boundaries, directory creation,
asynchronous concurrency, read_piece, and error handling.
"""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
import unittest
from pathlib import Path
from typing import List

from modules.fileWriter import FileWriter
from modules.torrentFileParser import FileInfo, TorrentFile


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


def _sha1(data: bytes) -> bytes:
    return hashlib.sha1(data).digest()


def _make_torrent(
    files: List[FileInfo],
    piece_length: int,
    name: bytes = b"test_torrent",
) -> TorrentFile:
    """Helper to construct a TorrentFile with consistent hashes."""
    total_length = sum(f.length for f in files)
    num_pieces = (total_length + piece_length - 1) // piece_length if total_length > 0 else 0
    piece_hashes = [b"\xaa" * 20 for _ in range(num_pieces)]

    return TorrentFile(
        announce="http://tracker.example.com/announce",
        info_hash=b"\xbb" * 20,
        piece_hashes=piece_hashes,
        piece_length=piece_length,
        files=files,
        name=name,
    )


class TestFileWriterSingleFile(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_single_file_write_in_order(self):
        """Write all pieces sequentially to a single file."""
        piece_len = 100
        total_len = 250  # 3 pieces: 100, 100, 50
        torrent = _make_torrent([FileInfo(path="single.bin", length=total_len)], piece_len)

        data0 = b"A" * 100
        data1 = b"B" * 100
        data2 = b"C" * 50

        async def run():
            writer = FileWriter(torrent, self.out_dir)
            await writer.write_piece(0, data0)
            await writer.write_piece(1, data1)
            await writer.write_piece(2, data2)
            await writer.close()

        _run(run())

        dest_file = self.out_dir / "single.bin"
        self.assertTrue(dest_file.exists())
        self.assertEqual(dest_file.read_bytes(), data0 + data1 + data2)

    def test_single_file_write_out_of_order(self):
        """Write pieces out of order (piece 2, then piece 0, then piece 1)."""
        piece_len = 64
        total_len = 160  # 3 pieces: 64, 64, 32
        torrent = _make_torrent([FileInfo(path="single_ooo.bin", length=total_len)], piece_len)

        p0 = b"\x01" * 64
        p1 = b"\x02" * 64
        p2 = b"\x03" * 32

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(2, p2)
                await writer.write_piece(0, p0)
                await writer.write_piece(1, p1)

        _run(run())

        dest = self.out_dir / "single_ooo.bin"
        self.assertEqual(dest.read_bytes(), p0 + p1 + p2)

    def test_read_piece_single_file(self):
        """Test read_piece returns the exact bytes previously written."""
        piece_len = 50
        total_len = 120  # 50, 50, 20
        torrent = _make_torrent([FileInfo(path="data.bin", length=total_len)], piece_len)

        data = b"0123456789" * 12

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(0, data[0:50])
                await writer.write_piece(1, data[50:100])
                await writer.write_piece(2, data[100:120])

                read0 = await writer.read_piece(0)
                read1 = await writer.read_piece(1)
                read2 = await writer.read_piece(2)

                self.assertEqual(read0, data[0:50])
                self.assertEqual(read1, data[50:100])
                self.assertEqual(read2, data[100:120])

        _run(run())


class TestFileWriterMultiFile(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_piece_spans_two_files(self):
        """A single piece crosses the boundary between file 1 and file 2."""
        files = [
            FileInfo(path="file1.txt", length=30),
            FileInfo(path="file2.txt", length=70),
        ]
        # 1 piece of size 100 covers both files
        torrent = _make_torrent(files, piece_length=100)

        data = b"1" * 30 + b"2" * 70

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(0, data)
                read_back = await writer.read_piece(0)
                self.assertEqual(read_back, data)

        _run(run())

        f1 = self.out_dir / "file1.txt"
        f2 = self.out_dir / "file2.txt"
        self.assertEqual(f1.read_bytes(), b"1" * 30)
        self.assertEqual(f2.read_bytes(), b"2" * 70)

    def test_piece_spans_three_files(self):
        """A large piece spans across file A, all of file B, and part of file C."""
        files = [
            FileInfo(path="a.bin", length=20),
            FileInfo(path="b.bin", length=15),
            FileInfo(path="c.bin", length=40),
        ]
        # Piece length 60:
        # Piece 0: 20 (a.bin) + 15 (b.bin) + 25 (c.bin) = 60
        # Piece 1: 15 (c.bin) = 15
        torrent = _make_torrent(files, piece_length=60)

        data_p0 = b"A" * 20 + b"B" * 15 + b"C" * 25
        data_p1 = b"c" * 15

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(1, data_p1)
                await writer.write_piece(0, data_p0)

                read_p0 = await writer.read_piece(0)
                read_p1 = await writer.read_piece(1)
                self.assertEqual(read_p0, data_p0)
                self.assertEqual(read_p1, data_p1)

        _run(run())

        self.assertEqual((self.out_dir / "a.bin").read_bytes(), b"A" * 20)
        self.assertEqual((self.out_dir / "b.bin").read_bytes(), b"B" * 15)
        self.assertEqual((self.out_dir / "c.bin").read_bytes(), b"C" * 25 + b"c" * 15)

    def test_nested_subdirectories_created(self):
        """Ensure parent directories are automatically created."""
        files = [
            FileInfo(path="dir1/sub/fileA.dat", length=40),
            FileInfo(path="dir2/fileB.dat", length=60),
        ]
        torrent = _make_torrent(files, piece_length=50)

        p0 = b"X" * 40 + b"Y" * 10
        p1 = b"Y" * 50

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(0, p0)
                await writer.write_piece(1, p1)

        _run(run())

        fA = self.out_dir / "dir1" / "sub" / "fileA.dat"
        fB = self.out_dir / "dir2" / "fileB.dat"
        self.assertTrue(fA.exists())
        self.assertTrue(fB.exists())
        self.assertEqual(fA.read_bytes(), b"X" * 40)
        self.assertEqual(fB.read_bytes(), b"Y" * 60)

    def test_empty_file_in_torrent(self):
        """0-byte file in files list should be created with 0 bytes."""
        files = [
            FileInfo(path="empty.txt", length=0),
            FileInfo(path="content.txt", length=50),
        ]
        torrent = _make_torrent(files, piece_length=50)

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                await writer.write_piece(0, b"Z" * 50)

        _run(run())

        empty = self.out_dir / "empty.txt"
        content = self.out_dir / "content.txt"
        self.assertTrue(empty.exists())
        self.assertEqual(empty.stat().st_size, 0)
        self.assertEqual(content.read_bytes(), b"Z" * 50)


class TestFileWriterConcurrency(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_concurrent_piece_writes(self):
        """Concurrent asyncio tasks writing pieces to the same file."""
        num_pieces = 20
        piece_len = 1024
        total_len = num_pieces * piece_len

        torrent = _make_torrent(
            [FileInfo(path="concurrent.bin", length=total_len)],
            piece_length=piece_len,
        )

        pieces_data = [bytes([(i * 7) % 256]) * piece_len for i in range(num_pieces)]

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                import random
                indices = list(range(num_pieces))
                random.shuffle(indices)

                tasks = [
                    writer.write_piece(idx, pieces_data[idx])
                    for idx in indices
                ]
                await asyncio.gather(*tasks)

                for i in range(num_pieces):
                    read = await writer.read_piece(i)
                    self.assertEqual(read, pieces_data[i])

        _run(run())

        full_file = self.out_dir / "concurrent.bin"
        self.assertEqual(full_file.read_bytes(), b"".join(pieces_data))


class TestFileWriterErrors(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_invalid_piece_index_raises(self):
        torrent = _make_torrent([FileInfo(path="f.txt", length=100)], piece_length=50)

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                with self.assertRaises(IndexError):
                    await writer.write_piece(-1, b"x" * 50)
                with self.assertRaises(IndexError):
                    await writer.write_piece(2, b"x" * 50)  # valid are 0, 1
                with self.assertRaises(IndexError):
                    await writer.read_piece(5)

        _run(run())

    def test_wrong_data_length_raises(self):
        torrent = _make_torrent([FileInfo(path="f.txt", length=100)], piece_length=50)

        async def run():
            async with FileWriter(torrent, self.out_dir) as writer:
                with self.assertRaises(ValueError):
                    await writer.write_piece(0, b"x" * 49)  # expected 50
                with self.assertRaises(ValueError):
                    await writer.write_piece(0, b"x" * 51)  # expected 50

        _run(run())

    def test_operations_on_closed_writer_raise(self):
        torrent = _make_torrent([FileInfo(path="f.txt", length=50)], piece_length=50)

        async def run():
            writer = FileWriter(torrent, self.out_dir)
            await writer.close()

            with self.assertRaises(RuntimeError):
                await writer.write_piece(0, b"x" * 50)

            with self.assertRaises(RuntimeError):
                await writer.read_piece(0)

        _run(run())

    def test_close_is_idempotent(self):
        torrent = _make_torrent([FileInfo(path="f.txt", length=50)], piece_length=50)

        async def run():
            writer = FileWriter(torrent, self.out_dir)
            await writer.close()
            await writer.close()  # should not raise
            writer.close_sync()   # should not raise

        _run(run())

    def test_sync_context_manager(self):
        torrent = _make_torrent([FileInfo(path="sync.txt", length=20)], piece_length=20)
        with FileWriter(torrent, self.out_dir) as writer:
            self.assertFalse(writer._closed)
        self.assertTrue(writer._closed)


if __name__ == "__main__":
    unittest.main()
