from crawler import DHTCrawler

initial_nodes = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

svr = DHTCrawler(initial_nodes)
svr.run()
