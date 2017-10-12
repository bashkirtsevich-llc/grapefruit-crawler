import asyncio

from protocol import DHTProtocol

initial_nodes = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

loop = asyncio.get_event_loop()

listen = loop.create_datagram_endpoint(
    lambda: DHTProtocol(loop, initial_nodes),
    local_addr=("0.0.0.0", 6981)
)

transport, protocol = loop.run_until_complete(listen)

try:
    loop.run_forever()
finally:
    transport.close()
    loop.close()
