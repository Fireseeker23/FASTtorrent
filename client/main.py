"""
main.py — FASTtorrent CLI: Real-time BitTorrent Downloader.

Demonstrates the entire BitTorrent client pipeline:
  - Decodes .torrent file with BencodeDecoder and TorrentFile
  - Announces to tracker(s) to fetch active swarm peers
  - Manages concurrent peer pool and protocol handshakes
  - Schedules blocks rarest-first and verifies SHA-1 piece hashes
  - Writes data across single or multi-file directory structures
  - Renders an in-place single-line progress bar with live speed and ETA
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
import time
from pathlib import Path

# Add client root to module search path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from modules.fileWriter import FileWriter
from modules.peerPool import PeerPoolManager
from modules.pieceManager import PieceManager
from modules.torrentFileParser import TorrentFile
from modules.trackCommunication import TrackerClient, TrackerError

# Terminal color constants
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
RED = "\033[31m"


def _enable_windows_vt() -> bool:
    """Enable virtual terminal processing on Windows and configure UTF-8 stdout."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            h_out = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            mode = ctypes.c_ulong()
            if kernel32.GetConsoleMode(h_out, ctypes.byref(mode)):
                kernel32.SetConsoleMode(h_out, mode.value | 0x0004)
                return True
        except Exception:
            pass
    return False


def _format_bytes(num_bytes: float) -> str:
    """Format bytes into a human-readable string (KB, MB, GB)."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def _format_time(seconds: float) -> str:
    """Format seconds into HH:MM:SS or MM:SS."""
    if seconds < 0 or seconds > 3600 * 24 * 7:
        return "--:--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _make_progress_bar(fraction: float, width: int = 14) -> str:
    """Generate a progress bar: [████████░░░░░░] with ASCII fallback if needed."""
    filled_len = int(width * fraction)
    filled_len = max(0, min(width, filled_len))
    try:
        "█░".encode(sys.stdout.encoding or "utf-8")
        fill_char, empty_char = "█", "░"
    except Exception:
        fill_char, empty_char = "#", "-"
    bar = fill_char * filled_len + empty_char * (width - filled_len)
    return f"[{bar}]"


async def run_downloader(
    torrent_path: str,
    output_dir: str,
    max_peers: int = 30,
    max_pieces: int | None = None,
) -> None:
    _enable_windows_vt()
    # 1. Parse .torrent
    print(f"\n{BOLD}{CYAN}=== FASTtorrent Downloader ==={RESET}\n")
    print(f"Loading metainfo from: {BOLD}{torrent_path}{RESET} ...")

    torrent = TorrentFile.from_path(torrent_path)
    total_bytes = torrent.total_length
    total_pieces = torrent.num_pieces
    target_pieces = min(total_pieces, max_pieces) if max_pieces else total_pieces

    print(f"  Name       : {BOLD}{torrent.name.decode(errors='replace')}{RESET}")
    print(f"  Info Hash  : {torrent.info_hash.hex()}")
    print(f"  Total Size : {_format_bytes(total_bytes)} across {len(torrent.files)} file(s)")
    print(f"  Pieces     : {total_pieces:,} ({torrent.piece_length // 1024} KB each)")
    print(f"  Output Dir : {output_dir}")
    if max_pieces:
        print(f"  Target     : Demo limit of {max_pieces} piece(s)")

    # 2. Setup storage and piece tracker
    writer = FileWriter(torrent, output_dir)
    piece_manager = PieceManager(
        piece_hashes=torrent.piece_hashes,
        piece_length=torrent.piece_length,
        total_length=torrent.total_length,
    )

    # 3. Piece completion callback (invoked when SHA-1 verified)
    downloaded_bytes = 0
    completed_pieces_count = 0
    done_event = asyncio.Event()

    async def on_piece_complete(piece_index: int, data: bytes) -> None:
        nonlocal downloaded_bytes, completed_pieces_count
        await writer.write_piece(piece_index, data)
        downloaded_bytes += len(data)
        completed_pieces_count += 1
        if max_pieces and completed_pieces_count >= max_pieces:
            done_event.set()
        elif piece_manager.is_done:
            done_event.set()

    # 4. Generate client Peer ID (-FT0001- + 12 random bytes)
    client_peer_id = b"-FT0001-" + os.urandom(12)

    # 5. Initialize peer pool
    pool = PeerPoolManager(
        piece_manager=piece_manager,
        info_hash=torrent.info_hash,
        peer_id=client_peer_id,
        on_piece_complete=on_piece_complete,
        max_peers=max_peers,
    )

    # 6. Announce to tracker
    tracker = TrackerClient()
    print(f"\nContacting tracker {BOLD}{torrent.announce}{RESET}...")
    try:
        tracker_resp = await tracker.announce(
            torrent=torrent,
            peer_id=client_peer_id,
            port=6881,
            uploaded=0,
            downloaded=0,
            left=torrent.total_length,
            event="started",
            numwant=50,
        )
    except TrackerError as err:
        print(f"{RED}Tracker announcement failed: {err}{RESET}")
        await writer.close()
        return

    print(f"Tracker response: {GREEN}{tracker_resp.seeders} seeds{RESET}, "
          f"{YELLOW}{tracker_resp.leechers} leechers{RESET}, "
          f"{len(tracker_resp.peers)} peers received.")

    # Connect to all peers returned by tracker (both IPv4 and IPv6) concurrently
    print(f"Connecting to {len(tracker_resp.peers)} peer(s) in parallel...\n")
    await pool.add_peers(tracker_resp.peers)

    # 7. Background re-announce loop to discover new peers over time
    async def reannounce_loop() -> None:
        interval = max(tracker_resp.interval, 30)
        while not done_event.is_set():
            try:
                await asyncio.sleep(interval)
                if done_event.is_set():
                    break
                resp = await tracker.announce(
                    torrent=torrent,
                    peer_id=client_peer_id,
                    port=6881,
                    uploaded=0,
                    downloaded=downloaded_bytes,
                    left=max(0, total_bytes - downloaded_bytes),
                    numwant=50,
                )
                if resp.peers:
                    await pool.add_peers(resp.peers)
            except Exception:
                pass

    reannounce_task = asyncio.create_task(reannounce_loop())

    # 8. Live real-time dashboard display loop
    start_time = time.time()
    last_time = start_time
    last_bytes = 0
    speeds: list[float] = []

    def render_dashboard(force: bool = False) -> None:
        nonlocal last_time, last_bytes
        now = time.time()
        dt = now - last_time
        if dt >= 0.3 or force:
            bytes_delta = downloaded_bytes - last_bytes
            current_speed = bytes_delta / dt if dt > 0 else 0
            speeds.append(current_speed)
            if len(speeds) > 6:
                speeds.pop(0)
            last_time = now
            last_bytes = downloaded_bytes

        avg_speed = sum(speeds) / len(speeds) if speeds else 0

        progress = completed_pieces_count / target_pieces if target_pieces else 0
        pct = progress * 100

        if completed_pieces_count >= target_pieces:
            progress = 1.0
            pct = 100.0
            eta_str = "00:00"
        else:
            remaining_bytes = (target_pieces - completed_pieces_count) * torrent.piece_length
            eta = remaining_bytes / avg_speed if avg_speed > 0 else -1
            eta_str = _format_time(eta)

        active_peers = pool.num_connected

        cols = shutil.get_terminal_size((80, 24)).columns
        max_cols = max(cols - 1, 40)

        # Dynamically fit the bar so the entire line NEVER wraps
        bar_w = max(6, min(14, max_cols - 56))
        bar = _make_progress_bar(progress, width=bar_w)

        display_pieces = min(completed_pieces_count, target_pieces) if target_pieces else completed_pieces_count
        status = (
            f"{bar} {pct:5.1f}% | "
            f"{_format_bytes(avg_speed)}/s | "
            f"{display_pieces}/{target_pieces} pcs | "
            f"{active_peers} peer(s) | "
            f"ETA:{eta_str}"
        )

        # Strictly cap length to max_cols to prevent terminal auto-wrap
        if len(status) > max_cols:
            status = status[:max_cols]
        padded = status.ljust(max_cols)

        sys.stdout.write(f"\r\033[K{padded}")
        sys.stdout.flush()

    async def dashboard_loop() -> None:
        while not done_event.is_set():
            render_dashboard()
            await asyncio.sleep(0.1)

    dash_task = asyncio.create_task(dashboard_loop())

    # Wait for completion or shutdown
    try:
        await asyncio.wait(
            [asyncio.create_task(done_event.wait()), asyncio.create_task(pool.wait_until_done())],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        pass
    finally:
        dash_task.cancel()
        reannounce_task.cancel()
        render_dashboard(force=True)
        sys.stdout.write("\n\n")
        sys.stdout.flush()
        print("Shutting down peer connections and flushing disk...")
        await pool.shutdown()
        await writer.close()

    total_time = time.time() - start_time
    avg_speed = downloaded_bytes / total_time if total_time > 0 else 0
    print(f"\n{BOLD}{GREEN}=== Download Complete ==={RESET}")
    print(f"  Downloaded : {_format_bytes(downloaded_bytes)} in {completed_pieces_count} piece(s)")
    print(f"  Time taken : {_format_time(total_time)}")
    print(f"  Avg speed  : {_format_bytes(avg_speed)}/s")
    print(f"  Output     : {os.path.abspath(output_dir)}\n")


def main() -> None:
    _enable_windows_vt()

    parser = argparse.ArgumentParser(
        prog="fasttorrent",
        description="FASTtorrent — High Performance Asynchronous BitTorrent Client",
    )
    parser.add_argument(
        "torrent",
        nargs="?",
        default="ubuntu-24.04.4-live-server-amd64.iso.torrent",
        help="Path to .torrent metainfo file",
    )
    parser.add_argument(
        "-o", "--output",
        default="./downloads",
        help="Destination directory for downloaded files (default: ./downloads)",
    )
    parser.add_argument(
        "-p", "--peers",
        type=int,
        default=30,
        help="Maximum simultaneous peer connections (default: 30)",
    )
    parser.add_argument(
        "-m", "--max-pieces",
        type=int,
        default=None,
        help="Limit number of pieces to download (useful for quick testing/demo)",
    )

    args = parser.parse_args()

    try:
        asyncio.run(
            run_downloader(
                torrent_path=args.torrent,
                output_dir=args.output,
                max_peers=args.peers,
                max_pieces=args.max_pieces,
            )
        )
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}Download interrupted by user.{RESET}")


if __name__ == "__main__":
    main()
