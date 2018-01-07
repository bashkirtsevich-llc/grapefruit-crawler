import asyncio
import logging
import os
from datetime import datetime
from itertools import product

import motor.motor_asyncio
from bt_utp import MicroTransportProtocol
from pymongo import ASCENDING

from crawler import DHTCrawler
from torrent import BitTorrentProtocol
from utils import hexlify, decode_bytes


class GrapefruitDHTCrawler(DHTCrawler):
    def __init__(self, db_url, db_name, **kwargs):
        super().__init__(**kwargs)

        client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = client[db_name]

        self.loop.run_until_complete(self.create_index())

        self.torrent_in_progress = set()  # For prevent multiple search same torrents
        self.protocols = ["tcp"]  # ["tcp", "utp"] -- experimental

    async def create_index(self):
        index = {
            "name": "info_hash",
            "keys": [("info_hash", ASCENDING)],
            "unique": True
        }

        coll = self.db.torrents
        if index["name"] not in await coll.index_information():
            await coll.create_index(**index)

    async def is_torrent_exists(self, info_hash):
        result = await self.db.torrents.count(filter={"info_hash": hexlify(info_hash)}) > 0
        return result

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

    async def create_connection(self, proto, host, port, info_hash, result_future):
        if proto == "utp":
            return await self.loop.create_datagram_endpoint(
                lambda: MicroTransportProtocol(BitTorrentProtocol(info_hash, result_future)),
                remote_addr=(host, port)
            )
        elif proto == "tcp":
            return await self.loop.create_connection(
                lambda: BitTorrentProtocol(info_hash, result_future),
                host=host, port=port
            )
        else:
            raise Exception("Unknown protocol '{}'".format(proto))

    async def connect_to_peer(self, peer, protocol, info_hash):
        result_future = self.loop.create_future()

        logging.debug(
            "Connect to\r\n"
            "\tprotocol: {}\r\n"
            "\tpeer: {}\r\n"
            "\tinfo_hash: {}".format(protocol, peer, hexlify(info_hash))
        )

        transport, _ = await self.create_connection(protocol, peer.host, peer.port, info_hash, result_future)
        result = await result_future

        logging.debug(
            "Got torrent metadata\r\n"
            "\tprotocol: {}\r\n"
            "\tpeer: {}\r\n"
            "\tinfo_hash: {}\r\n".format(protocol, peer, hexlify(info_hash))
        )
        return result

    async def wait_for_torrent(self, info_hash, peers):
        logging.debug(
            "Connect with torrent peers\r\n"
            "\tinfo_hash: {}\r\n"
            "\tpeers: {}".format(hexlify(info_hash), peers)
        )

        # Wait for 1 minute for torrent completion
        done, _ = await asyncio.wait([
            self.connect_to_peer(peer, protocol, info_hash)
            for peer, protocol in product(peers, self.protocols)
        ], timeout=60.0, return_when=asyncio.FIRST_COMPLETED, loop=self.loop)

        if not done:
            return None

        task = done.pop()
        if task.exception() is None:
            return task.result()

        return None

    async def connect_with_peers(self, info_hash, peers):
        for i in range(0, len(peers), 20):
            torrent = await self.wait_for_torrent(info_hash, peers[i: i + 20])
            if torrent:
                await self.save_torrent(info_hash, torrent)
                break

        if info_hash in self.torrent_in_progress:
            self.torrent_in_progress.remove(info_hash)

    async def enqueue_torrent(self, info_hash):
        has_torrent = await self.is_torrent_exists(info_hash)

        if info_hash not in self.torrent_in_progress and not has_torrent:
            logging.debug(
                "Enqueue search peers for torrent\r\n"
                "\tinfo_hash: {}".format(hexlify(info_hash)))

            self.torrent_in_progress.add(info_hash)
            await self.search_peers(info_hash)

    async def get_peers_received(self, node_id, info_hash, addr):
        await self.enqueue_torrent(info_hash)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        await self.enqueue_torrent(info_hash)

    async def peers_values_received(self, info_hash, peers):
        asyncio.ensure_future(
            self.connect_with_peers(
                # Ignore odd peers with bad port
                info_hash, [peer for peer in peers if peer.port >= 1024]
            ), loop=self.loop
        )


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

    logging.basicConfig(level=logging.DEBUG)

    svr = GrapefruitDHTCrawler(db_url, db_name, loop=loop, bootstrap_nodes=initial_nodes, interval=0.2)
    svr.run()
