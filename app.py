import asyncio

from protocol import DHTProtocol

loop = asyncio.get_event_loop()
listen = loop.create_datagram_endpoint(DHTProtocol, local_addr=("0.0.0.0", 6981))
transport, protocol = loop.run_until_complete(listen)

try:
    loop.run_forever()
finally:
    transport.close()
    loop.close()
