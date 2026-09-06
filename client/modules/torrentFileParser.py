import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

from modules.bencode import BencodeDecoder, BencodeDecodeError


class TorrentParseError(ValueError):
    """Raised when a .torrent file is malformed or missing required fields."""
    pass


@dataclass
class FileInfo:
    """Represents a single file within a (possibly multi-file) torrent."""
    path: str          # Relative path as a joined string e.g. "subdir/file.txt"
    length: int        # File size in bytes


class TorrentFile:
    """
    Parses and represents a .torrent metainfo file.

    Supports both single-file and multi-file torrents.
    All string fields decoded from the info dict are kept as bytes so that
    hashing is unambiguous; callers can decode with a known codec if needed.
    """

    def __init__(
        self,
        announce: str,
        info_hash: bytes,
        piece_hashes: List[bytes],
        piece_length: int,
        files: List[FileInfo],
        name: bytes,
        announce_list: Optional[List[List[str]]] = None,
        comment: Optional[str] = None,
        created_by: Optional[str] = None,
        creation_date: Optional[int] = None,
        is_private: bool = False,
    ) -> None:
        self.announce = announce
        self.info_hash = info_hash
        self.piece_hashes = piece_hashes
        self.piece_length = piece_length
        self.files = files
        self.name = name
        self.announce_list = announce_list or []
        self.comment = comment
        self.created_by = created_by
        self.creation_date = creation_date
        self.is_private = is_private

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def total_length(self) -> int:
        """Total download size in bytes across all files."""
        return sum(f.length for f in self.files)

    @property
    def num_pieces(self) -> int:
        return len(self.piece_hashes)

    @property
    def is_multi_file(self) -> bool:
        return len(self.files) > 1

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @staticmethod
    def from_path(path: Union[str, Path]) -> "TorrentFile":
        """
        Parse a .torrent file from *path* and return a populated TorrentFile.

        Raises:
            FileNotFoundError: if the file does not exist.
            TorrentParseError: if the metainfo is malformed.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Torrent file not found: {path}")

        raw: bytes = path.read_bytes()

        try:
            meta: dict = BencodeDecoder.decode_data(raw)
        except BencodeDecodeError as exc:
            raise TorrentParseError(f"Failed to bencode-decode {path}: {exc}") from exc

        if not isinstance(meta, dict):
            raise TorrentParseError("Metainfo is not a bencoded dictionary")

        return TorrentFile._from_meta(meta, raw)

    @staticmethod
    def from_bytes(data: bytes) -> "TorrentFile":
        """Parse a .torrent file from raw *data* bytes."""
        try:
            meta: dict = BencodeDecoder.decode_data(data)
        except BencodeDecodeError as exc:
            raise TorrentParseError(f"Failed to bencode-decode torrent data: {exc}") from exc

        if not isinstance(meta, dict):
            raise TorrentParseError("Metainfo is not a bencoded dictionary")

        return TorrentFile._from_meta(meta, data)

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _from_meta(meta: dict, raw_meta_bytes: bytes) -> "TorrentFile":
        """Build a TorrentFile from an already-decoded metainfo dict."""

        def _get(d: dict, key: str, label: str):
            """Fetch a required key (tries both bytes and str keys)."""
            value = d.get(key.encode()) or d.get(key)
            if value is None:
                raise TorrentParseError(f"Missing required field: '{label}'")
            return value

        def _get_opt(d: dict, key: str):
            return d.get(key.encode()) or d.get(key)

        # ── Top-level fields ──────────────────────────────────────────
        announce_raw = _get(meta, "announce", "announce")
        announce = (
            announce_raw.decode("utf-8", errors="replace")
            if isinstance(announce_raw, (bytes, bytearray))
            else str(announce_raw)
        )

        # Optional announce-list (BEP-12 multi-tracker extension)
        announce_list_raw = _get_opt(meta, "announce-list")
        announce_list: List[List[str]] = []
        if announce_list_raw and isinstance(announce_list_raw, list):
            for tier in announce_list_raw:
                if isinstance(tier, list):
                    announce_list.append([
                        url.decode("utf-8", errors="replace") if isinstance(url, (bytes, bytearray)) else str(url)
                        for url in tier
                    ])

        comment_raw = _get_opt(meta, "comment")
        comment = (
            comment_raw.decode("utf-8", errors="replace")
            if isinstance(comment_raw, (bytes, bytearray))
            else (str(comment_raw) if comment_raw is not None else None)
        )

        created_by_raw = _get_opt(meta, "created by")
        created_by = (
            created_by_raw.decode("utf-8", errors="replace")
            if isinstance(created_by_raw, (bytes, bytearray))
            else (str(created_by_raw) if created_by_raw is not None else None)
        )

        creation_date = _get_opt(meta, "creation date")

        # ── Info dictionary ───────────────────────────────────────────
        info: dict = _get(meta, "info", "info")
        if not isinstance(info, dict):
            raise TorrentParseError("'info' field is not a dictionary")

        info_hash = TorrentFile._compute_info_hash(meta, raw_meta_bytes)

        # ── Piece data ────────────────────────────────────────────────
        piece_length: int = _get(info, "piece length", "info.piece length")
        if not isinstance(piece_length, int) or piece_length <= 0:
            raise TorrentParseError(f"Invalid piece length: {piece_length!r}")

        pieces_raw: bytes = _get(info, "pieces", "info.pieces")
        if not isinstance(pieces_raw, (bytes, bytearray)):
            raise TorrentParseError("'info.pieces' must be a byte string")
        if len(pieces_raw) % 20 != 0:
            raise TorrentParseError(
                f"'info.pieces' length {len(pieces_raw)} is not a multiple of 20"
            )
        piece_hashes: List[bytes] = [
            pieces_raw[i : i + 20] for i in range(0, len(pieces_raw), 20)
        ]

        # ── Name ──────────────────────────────────────────────────────
        name_raw: bytes = _get(info, "name", "info.name")
        name = name_raw if isinstance(name_raw, (bytes, bytearray)) else str(name_raw).encode()

        # ── Private flag (BEP-27) ─────────────────────────────────────
        is_private = bool(_get_opt(info, "private"))

        # ── Files ─────────────────────────────────────────────────────
        files_raw = _get_opt(info, "files")

        if files_raw is not None:
            # Multi-file torrent
            if not isinstance(files_raw, list):
                raise TorrentParseError("'info.files' must be a list")
            files = TorrentFile._parse_multi_file(files_raw, name)
        else:
            # Single-file torrent
            length = _get(info, "length", "info.length")
            if not isinstance(length, int) or length < 0:
                raise TorrentParseError(f"Invalid file length: {length!r}")
            file_name = name.decode("utf-8", errors="replace")
            files = [FileInfo(path=file_name, length=length)]

        return TorrentFile(
            announce=announce,
            info_hash=info_hash,
            piece_hashes=piece_hashes,
            piece_length=piece_length,
            files=files,
            name=name,
            announce_list=announce_list,
            comment=comment,
            created_by=created_by,
            creation_date=creation_date,
            is_private=is_private,
        )

    @staticmethod
    def _parse_multi_file(files_raw: list, torrent_name: bytes) -> List[FileInfo]:
        """Parse the 'files' list from a multi-file torrent info dict."""
        files: List[FileInfo] = []
        root = torrent_name.decode("utf-8", errors="replace")

        for entry in files_raw:
            if not isinstance(entry, dict):
                raise TorrentParseError("Each entry in 'info.files' must be a dict")

            length = entry.get(b"length") or entry.get("length")
            if length is None or not isinstance(length, int) or length < 0:
                raise TorrentParseError(f"Invalid or missing 'length' in file entry: {entry!r}")

            path_parts_raw = entry.get(b"path") or entry.get("path")
            if not isinstance(path_parts_raw, list) or len(path_parts_raw) == 0:
                raise TorrentParseError(f"Invalid or missing 'path' in file entry: {entry!r}")

            path_parts = [
                part.decode("utf-8", errors="replace") if isinstance(part, (bytes, bytearray)) else str(part)
                for part in path_parts_raw
            ]
            # Build a relative path rooted at the torrent name directory
            relative_path = "/".join([root] + path_parts)

            files.append(FileInfo(path=relative_path, length=length))

        return files

    @staticmethod
    def _compute_info_hash(meta: dict, raw_meta_bytes: bytes) -> bytes:
        """
        Compute the SHA-1 info_hash.

        Strategy: find the bencoded 'info' value within raw_meta_bytes by
        locating the byte offset of the 'info' key, then SHA-1 that slice.
        This avoids re-encoding (which would require a complete BencodeEncoder
        round-trip and could alter key ordering).
        """
        # The bencoded key for "info" is b"4:info"
        info_key = b"4:info"
        start = raw_meta_bytes.find(info_key)
        if start == -1:
            raise TorrentParseError("Cannot locate 'info' key in raw torrent bytes for hashing")

        # The info value starts immediately after the key
        info_value_start = start + len(info_key)

        # We need to find the end of the bencoded info dict.
        # Re-decode from that offset to measure its length.
        info_slice = raw_meta_bytes[info_value_start:]
        # Walk the decoder to find where the info dict ends
        decoder = BencodeDecoder(info_slice)
        decoder._parse_next()  # consume exactly the info value
        info_value_end = info_value_start + decoder._index

        info_bytes = raw_meta_bytes[info_value_start:info_value_end]
        return hashlib.sha1(info_bytes).digest()

    def calculate_info_hash(self) -> bytes:
        """Return the cached 20-byte SHA-1 info_hash."""
        return self.info_hash

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        name_str = self.name.decode("utf-8", errors="replace")
        return (
            f"TorrentFile("
            f"name={name_str!r}, "
            f"announce={self.announce!r}, "
            f"pieces={self.num_pieces}, "
            f"piece_length={self.piece_length}, "
            f"total_length={self.total_length}, "
            f"files={len(self.files)}, "
            f"info_hash={self.info_hash.hex()!r}"
            f")"
        )