import os


def run_server(server_factory):
    initial_nodes = [
        ("67.215.246.10", 6881),  # router.bittorrent.com
        ("87.98.162.88", 6881),  # dht.transmissionbt.com
        ("82.221.103.244", 6881)  # router.utorrent.com
    ]

    socket_host = os.getenv("SOCKET_HOST", "0.0.0.0")
    socket_port = int(os.getenv("SOCKET_PORT", "6881"))

    miner_interval = float(os.getenv("MINER_INTERVAL", "0.1"))
    download_bandwidth = int(os.getenv("DOWNLOAD_BANDWIDTH", "0"))
    upload_bandwidth = int(os.getenv("UPLOAD_BANDWIDTH", "0"))

    server = server_factory(
        bootstrap_nodes=initial_nodes,
        miner_interval=miner_interval,
        download_speed=download_bandwidth,
        upload_speed=upload_bandwidth
    )
    server.run(
        host=socket_host,
        port=socket_port
    )


if __name__ == '__main__':
    crawler_name = os.getenv("CRAWLER_WRITER", "file")

    if crawler_name == "file":
        from crawler_file import TorrentCrawlerFile

        run_server(
            lambda **kwargs: TorrentCrawlerFile(
                folder_path=os.getenv("TORRENTS_FOLDER"),
                **kwargs
            )
        )

    elif crawler_name == "mongo":
        from crawler_mongo import TorrentCrawlerMongo

        run_server(
            lambda **kwargs: TorrentCrawlerMongo(
                db_url=os.getenv("MONGO_URI"),
                db_name=os.getenv("MONGO_DB_NAME"),
                **kwargs
            )
        )

    else:
        print("Wrong 'CRAWLER_WRITER' value")
