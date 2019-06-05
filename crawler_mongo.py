from datetime import datetime

import motor.motor_asyncio
from pymongo import ASCENDING

from crawler import TorrentCrawler
from utils import hexlify, decode_bytes


class TorrentCrawlerMongo(TorrentCrawler):
    def __init__(self, db_url, db_name, **kwargs):
        super().__init__(**kwargs)

        client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = client[db_name]
        self.loop.run_until_complete(self.create_index())

    async def create_index(self):
        index = {
            "name": "info_hash",
            "keys": [("info_hash", ASCENDING)],
            "unique": True
        }

        coll = self.db.torrents
        if index["name"] not in await coll.index_information():
            await coll.create_index(**index)

    async def enqueue_torrent(self, info_hash):
        if await self.db.torrents.count(filter={"info_hash": hexlify(info_hash)}) == 0:
            await super(TorrentCrawlerMongo, self).enqueue_torrent(info_hash)

    async def save_torrent(self, info_hash, torrent):
        if "files" in torrent:
            files = torrent["files"]
        else:
            files = [{"length": torrent["length"], "path": [torrent["name"]]}]

        metadata = {
            "info_hash": hexlify(info_hash),
            "files": decode_bytes(files),
            "name": decode_bytes(torrent["name"]),
            "timestamp": datetime.now()
        }

        await self.db.torrents.insert_one(metadata)
