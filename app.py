import asyncio
import os
from binascii import hexlify

import motor.motor_asyncio
from pymongo import ASCENDING

from crawler import DHTCrawler


class GrapefruitDHTCrawler(DHTCrawler):
    def __init__(self, db_url, db_name, **kwargs):
        client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = client[db_name]

        super().__init__(**kwargs)

        self.loop.run_until_complete(self.create_indexes())

    def hexlify_info_hash(self, info_hash):
        return str(hexlify(info_hash), "utf-8")

    async def create_indexes(self):
        index_info = {"name": "info_hash", "keys": [("info_hash", ASCENDING)], "unique": True}

        hashes = self.db.hashes
        hashes_indexes = await hashes.index_information()

        if index_info["name"] not in hashes_indexes:
            await hashes.create_index(**index_info)

    async def store_info_hash(self, info_hash):
        info_hash_hex = self.hexlify_info_hash(info_hash)
        if await self.db.hashes.count(filter={"info_hash": info_hash_hex}) == 0:
            await self.db.hashes.insert_one({"info_hash": info_hash_hex})

    async def is_peers_needed(self, info_hash):
        info_hash_hex = self.hexlify_info_hash(info_hash)
        result = await self.db.hashes.count(filter={"info_hash": info_hash_hex})

        return result == 0

    async def get_peers_received(self, node_id, info_hash, addr):
        if await self.is_peers_needed(info_hash):
            await self.add_peers_searcher(info_hash)

        await self.store_info_hash(info_hash)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        if await self.is_peers_needed(info_hash):
            await self.add_peers_searcher(info_hash)

        await self.store_info_hash(info_hash)

    async def peers_values_received(self, info_hash, peers):
        print("peers for", self.hexlify_info_hash(info_hash), peers)


if __name__ == '__main__':
    db_url = os.environ["MONGODB_URL"]
    db_name = os.getenv("MONGODB_BASE_NAME", "grapefruit")

    initial_nodes = [
        ("router.bittorrent.com", 6881),
        ("dht.transmissionbt.com", 6881),
        ("router.utorrent.com", 6881)
    ]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    svr = GrapefruitDHTCrawler(db_url, db_name, loop=loop, bootstrap_nodes=initial_nodes, interval=0.1)
    svr.run()
