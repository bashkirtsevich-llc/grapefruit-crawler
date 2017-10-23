import os
import motor.motor_asyncio
from crawler import DHTCrawler
from binascii import hexlify


class GrapefruitDHTCrawler(DHTCrawler):
    def __init__(self, db_url, db_name, bootstrap_nodes, node_id=None, loop=None, interval=0.001):
        client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = client[db_name]

        super().__init__(bootstrap_nodes, node_id, loop, interval)

    async def store_info_hash(self, info_hash):
        info_hash_hex = str(hexlify(info_hash), "utf-8")

        if await self.db.hashes.count(filter={"info_hash": info_hash_hex}) == 0:
            await self.db.hashes.insert_one({"info_hash": info_hash_hex})

    async def get_peers_received(self, node_id, info_hash, addr):
        await self.store_info_hash(info_hash)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        await self.store_info_hash(info_hash)


try:
    import local
except ImportError:
    pass

db_url = os.environ["MONGODB_URL"]
db_name = os.getenv("MONGODB_BASE_NAME", "grapefruit")

initial_nodes = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

svr = GrapefruitDHTCrawler(db_url, db_name, initial_nodes)
svr.run()
