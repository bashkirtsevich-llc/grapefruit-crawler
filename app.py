import asyncio
import os
import motor.motor_asyncio
from pymongo import ASCENDING
from crawler import DHTCrawler
from binascii import hexlify


class GrapefruitDHTCrawler(DHTCrawler):
    def __init__(self, db_url, db_name, **kwargs):
        client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = client[db_name]

        super().__init__(**kwargs)

        self.loop.run_until_complete(self.create_indexes())

    async def create_indexes(self):
        index_info = {"name": "info_hash", "keys": [("info_hash", ASCENDING)], "unique": True}

        hashes = self.db.hashes
        hashes_indexes = await hashes.index_information()

        if index_info["name"] not in hashes_indexes:
            await hashes.create_index(**index_info)

    async def store_info_hash(self, info_hash):
        info_hash_hex = str(hexlify(info_hash), "utf-8")

        if await self.db.hashes.count(filter={"info_hash": info_hash_hex}) == 0:
            await self.db.hashes.insert_one({"info_hash": info_hash_hex})

    async def get_peers_received(self, node_id, info_hash, addr):
        await self.store_info_hash(info_hash)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        await self.store_info_hash(info_hash)


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
