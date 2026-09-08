"""
live_stats.py — Query the Ubuntu tracker and print live swarm stats.

Usage:
    uv run python live_stats.py
    uv run python live_stats.py path/to/other.torrent
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modules.torrentFileParser import TorrentFile
from modules.trackCommunication import TrackerClient, TrackerError


async def main(torrent_path: str):
    torrent = TorrentFile.from_path(torrent_path)
    client  = TrackerClient()

    print()
    print("=" * 60)
    print("  Torrent Info")
    print("=" * 60)
    print(f"  Name      : {torrent.name.decode()}")
    print(f"  Info hash : {torrent.info_hash.hex()}")
    print(f"  Size      : {torrent.total_length / 1024**3:.2f} GB")
    print(f"  Pieces    : {torrent.num_pieces:,}  ({torrent.piece_length // 1024} KB each)")
    print(f"  Files     : {len(torrent.files)}")
    print(f"  Private   : {torrent.is_private}")
    print(f"  Tracker   : {torrent.announce}")
    if torrent.announce_list:
        print(f"  Backups   :")
        for tier in torrent.announce_list:
            for url in tier:
                if url != torrent.announce:
                    print(f"              {url}")
    print()

    print("  Announcing to tracker...")
    print()

    try:
        resp = await client.announce(
            torrent=torrent,
            peer_id=b"-AG0001-" + b"x" * 12,
            port=6881,
            uploaded=0,
            downloaded=0,
            left=torrent.total_length,
            event="started",
            numwant=50,
        )
    except TrackerError as e:
        print(f"  ERROR: {e}")
        return

    print("=" * 60)
    print("  Live Swarm Stats")
    print("=" * 60)
    print(f"  Seeders   : {resp.seeders}")
    print(f"  Leechers  : {resp.leechers}")
    print(f"  Peers got : {len(resp.peers)}")
    print(f"  Interval  : {resp.interval}s  (re-announce every {resp.interval // 60} min)")
    if resp.min_interval:
        print(f"  Min intv. : {resp.min_interval}s")
    if resp.warning_message:
        print(f"  Warning   : {resp.warning_message}")
    print()
    print(f"  Peer List:")
    print("  " + "-" * 40)
    for peer in resp.peers:
        print(f"    {peer.ip}:{peer.port}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "ubuntu-24.04.4-live-server-amd64.iso.torrent"
    asyncio.run(main(path))
