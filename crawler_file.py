import os
from crawler import TorrentCrawler
from utils import hexlify


class TorrentCrawlerFile(TorrentCrawler):
    def __init__(self, folder_path, **kwargs):
        super().__init__(**kwargs)

        self.folder_path = folder_path

    def get_path_for_torrent(self, info_hash):
        return os.path.join(self.folder_path, hexlify(info_hash))

    async def enqueue_torrent(self, info_hash):
        if not os.path.exists(self.get_path_for_torrent(info_hash)):
            await super(TorrentCrawlerFile, self).enqueue_torrent(info_hash)

    async def save_torrent(self, info_hash, torrent):
        with open(self.get_path_for_torrent(info_hash), "wb") as file:
            file.write(torrent)
