from crawler import DHTCrawler

initial_nodes = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]


class GrapefruitDHTCrawler(DHTCrawler):
    async def get_peers_received(self, node_id, info_hash, addr):
        print("get_peers", node_id, info_hash, addr)

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        print("announce_peer", node_id, info_hash, port, addr)


svr = GrapefruitDHTCrawler(initial_nodes)
svr.run()
