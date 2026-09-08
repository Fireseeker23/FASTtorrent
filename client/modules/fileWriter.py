"""
fileWriter.py — Multi-file and single-file disk writer for BitTorrent pieces.

Maps piece-level data (received and verified from peers) across the file structure
defined in the torrent metainfo, handling pieces that span across multiple files
seamlessly.

Key capabilities:
  - Single-file and multi-file torrent layouts
  - Automatic subdirectory creation
  - Thread-safe seek & write across file boundaries
  - Asynchronous non-blocking disk I/O (via asyncio.to_thread)
  - read_piece for integrity verification and seeding
  - Async context manager support
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple, Union

from modules.torrentFileParser import FileInfo, TorrentFile

logger = logging.getLogger(__name__)


@dataclass
class FileSpan:
    """Represents a file's location in the global torrent byte stream."""
    info: FileInfo
    full_path: Path
    global_start: int
    global_end: int

    @property
    def length(self) -> int:
        return self.info.length


class FileWriter:
    """
    Manages disk storage for downloaded torrent pieces.

    Usage::

        writer = FileWriter(torrent, output_dir="./downloads")
        await writer.write_piece(piece_index, piece_data)
        ...
        await writer.close()
    """

    def __init__(self, torrent: TorrentFile, output_dir: Union[str, Path] = ".") -> None:
        self.torrent = torrent
        self.output_dir = Path(output_dir).resolve()

        # Compute global byte spans for each file
        self.spans: List[FileSpan] = []
        current_offset = 0
        for file_info in self.torrent.files:
            full_path = self.output_dir / file_info.path
            span = FileSpan(
                info=file_info,
                full_path=full_path,
                global_start=current_offset,
                global_end=current_offset + file_info.length,
            )
            self.spans.append(span)
            current_offset += file_info.length

        # Open file handles: file_path_str -> BinaryIO
        self.files: Dict[str, BinaryIO] = {}
        # Per-file thread lock for safe seek/write/read
        self._file_locks: Dict[str, threading.Lock] = {}
        self._closed: bool = False

        # Open and pre-initialize all destination files
        self._open_files()

    # ------------------------------------------------------------------
    # File handle lifecycle
    # ------------------------------------------------------------------

    def _open_files(self) -> None:
        """Create parent directories and open all files in r+b or wb+ mode."""
        for span in self.spans:
            key = str(span.full_path)
            if key not in self.files:
                span.full_path.parent.mkdir(parents=True, exist_ok=True)
                if span.full_path.exists():
                    handle = open(span.full_path, "r+b")
                else:
                    handle = open(span.full_path, "wb+")
                self.files[key] = handle
                self._file_locks[key] = threading.Lock()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("FileWriter is closed")
        if not self.files and self.spans:
            self._open_files()

    async def close(self) -> None:
        """Flush and close all open file handles asynchronously."""
        if self._closed:
            return
        self._closed = True

        def _do_close(handles: List[BinaryIO]) -> None:
            for h in handles:
                try:
                    h.flush()
                    h.close()
                except Exception as exc:
                    logger.debug("Error closing file: %s", exc)

        await asyncio.to_thread(_do_close, list(self.files.values()))
        self.files.clear()
        self._file_locks.clear()

    def close_sync(self) -> None:
        """Synchronously flush and close all file handles."""
        if self._closed:
            return
        self._closed = True
        for h in self.files.values():
            try:
                h.flush()
                h.close()
            except Exception:
                pass
        self.files.clear()
        self._file_locks.clear()

    async def __aenter__(self) -> FileWriter:
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __enter__(self) -> FileWriter:
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_sync()

    # ------------------------------------------------------------------
    # Writing pieces
    # ------------------------------------------------------------------

    async def write_piece(self, piece_index: int, data: bytes) -> None:
        """
        Write a completed piece's raw bytes to the appropriate file(s).

        Pieces that span multiple files are partitioned and written across the
        respective files at the correct offsets.

        Args:
            piece_index: 0-indexed piece number.
            data: Raw piece bytes.

        Raises:
            IndexError: if piece_index is out of range.
            ValueError: if data length exceeds bounds.
            RuntimeError: if writer is closed.
        """
        self._ensure_open()

        if piece_index < 0 or piece_index >= self.torrent.num_pieces:
            raise IndexError(
                f"Piece index {piece_index} out of range (0..{self.torrent.num_pieces - 1})"
            )

        piece_start = piece_index * self.torrent.piece_length
        piece_end = piece_start + len(data)

        if piece_end > self.torrent.total_length:
            raise ValueError(
                f"Piece {piece_index} with length {len(data)} exceeds "
                f"torrent total length ({self.torrent.total_length})"
            )

        # Expected piece length for this index
        expected_len = min(
            self.torrent.piece_length,
            self.torrent.total_length - piece_start,
        )
        if len(data) != expected_len:
            raise ValueError(
                f"Piece {piece_index} expected length {expected_len}, got {len(data)}"
            )

        tasks = []
        for span in self.spans:
            overlap_start = max(piece_start, span.global_start)
            overlap_end = min(piece_end, span.global_end)

            if overlap_start < overlap_end:
                data_start = overlap_start - piece_start
                data_len = overlap_end - overlap_start
                chunk = data[data_start : data_start + data_len]
                file_offset = overlap_start - span.global_start

                key = str(span.full_path)
                handle = self.files[key]
                lock = self._file_locks[key]

                tasks.append(
                    asyncio.to_thread(self._write_chunk, handle, lock, file_offset, chunk)
                )

        if tasks:
            await asyncio.gather(*tasks)

        logger.debug("Piece %d (%d bytes) written to disk", piece_index, len(data))

    @staticmethod
    def _write_chunk(
        handle: BinaryIO, lock: threading.Lock, offset: int, chunk: bytes
    ) -> None:
        with lock:
            handle.seek(offset)
            handle.write(chunk)
            handle.flush()

    # ------------------------------------------------------------------
    # Reading pieces / verification
    # ------------------------------------------------------------------

    async def read_piece(
        self, piece_index: int, length: Optional[int] = None
    ) -> bytes:
        """
        Read a piece's raw bytes from disk across file boundaries.

        Args:
            piece_index: 0-indexed piece number.
            length: Number of bytes to read (defaults to expected piece length).

        Returns:
            The raw piece bytes read from disk.
        """
        self._ensure_open()

        if piece_index < 0 or piece_index >= self.torrent.num_pieces:
            raise IndexError(
                f"Piece index {piece_index} out of range (0..{self.torrent.num_pieces - 1})"
            )

        piece_start = piece_index * self.torrent.piece_length
        if length is None:
            length = min(
                self.torrent.piece_length,
                self.torrent.total_length - piece_start,
            )

        piece_end = piece_start + length
        if piece_end > self.torrent.total_length:
            raise ValueError(
                f"Read request for piece {piece_index} ({piece_start}..{piece_end}) "
                f"exceeds torrent total length ({self.torrent.total_length})"
            )

        chunks: List[Tuple[int, bytes]] = []

        for span in self.spans:
            overlap_start = max(piece_start, span.global_start)
            overlap_end = min(piece_end, span.global_end)

            if overlap_start < overlap_end:
                data_start = overlap_start - piece_start
                data_len = overlap_end - overlap_start
                file_offset = overlap_start - span.global_start

                key = str(span.full_path)
                handle = self.files[key]
                lock = self._file_locks[key]

                chunk = await asyncio.to_thread(
                    self._read_chunk, handle, lock, file_offset, data_len
                )
                chunks.append((data_start, chunk))

        chunks.sort(key=lambda c: c[0])
        return b"".join(c[1] for c in chunks)

    @staticmethod
    def _read_chunk(
        handle: BinaryIO, lock: threading.Lock, offset: int, nbytes: int
    ) -> bytes:
        with lock:
            handle.seek(offset)
            data = handle.read(nbytes)
            # If EOF reached early, pad with zeroes to maintain length
            if len(data) < nbytes:
                data = data + b"\x00" * (nbytes - len(data))
            return data