import asyncio

from spyder import DHTSpyder
from torrent import BitTorrentProtocol


class TorrentCrawler(DHTSpyder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.torrent_in_progress = set()  # For prevent multiple search same torrents

    async def create_connection(self, host, port, info_hash, result_future):
        return await self.loop.create_connection(
            lambda: BitTorrentProtocol(info_hash, result_future),
            host=host, port=port
        )

    async def connect_to_peer(self, peer, info_hash):
        try:
            result_future = self.loop.create_future()
            await self.create_connection(peer.host, peer.port, info_hash, result_future)
            return await result_future
        except:
            return None

    async def wait_for_torrent(self, info_hash, peers):
        # Wait for 1 minute for torrent completion
        done, pending = await asyncio.wait([
            self.connect_to_peer(peer, info_hash)
            for peer in peers
        ], timeout=60.0, return_when=asyncio.FIRST_COMPLETED, loop=self.loop)

        for task in pending:
            task.cancel()

        return await done.pop() if done else None

    async def connect_with_peers(self, info_hash, peers):
        for i in range(0, len(peers), 20):
            try:
                torrent = await self.wait_for_torrent(info_hash, peers[i: i + 20])
            except:
                continue
            else:
                if torrent:
                    asyncio.ensure_future(self.save_torrent(info_hash, torrent), loop=self.loop)
                    break

        if info_hash in self.torrent_in_progress:
            self.torrent_in_progress.remove(info_hash)

    async def enqueue_torrent(self, info_hash):
        if info_hash not in self.torrent_in_progress:
            self.torrent_in_progress.add(info_hash)
            self.search_peers(info_hash)

    async def get_peers_received(self, node_id, info_hash, addr):
        await self.enqueue_torrent(info_hash)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        await self.enqueue_torrent(info_hash)

    async def peers_values_received(self, info_hash, peers):
        await self.connect_with_peers(info_hash, list(peers))

    async def save_torrent(self, info_hash, torrent):
        pass
