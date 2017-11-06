import asyncio
from heapq import nsmallest
from random import sample

from bencode import bencode, bdecode

from utils import generate_node_id, generate_id, get_routing_table_index, xor, decode_nodes, encode_nodes, get_rand_bool


class DHTCrawler(asyncio.DatagramProtocol):
    def __init__(self, bootstrap_nodes, node_id=None, loop=None, interval=0.001):
        self.node_id = node_id or generate_node_id()
        self.loop = loop or asyncio.get_event_loop()
        self.bootstrap_nodes = bootstrap_nodes
        self.interval = interval

        self.routing_table = [set() for _ in range(160)]

        self.transport = None
        self.__running = False

    def datagram_received(self, data, addr):
        try:
            msg = bdecode(data)
        except:
            return

        try:
            self.handle_message(msg, addr)
        except:
            response = {
                "y": "e",
                "e": [202, "Server Error"]
            }

            if "t" in msg:
                response["t"] = msg["t"]

            self.send_message(response, addr)

            raise

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.stop()
        self.transport.close()

    def run(self, host="0.0.0.0", port=6881):
        coro = self.loop.create_datagram_endpoint(lambda: self, local_addr=(host, port))
        transport, _ = self.loop.run_until_complete(coro)

        # Bootstrap
        for node in self.bootstrap_nodes:
            self.find_node(node, self.node_id)

        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def stop(self):
        self.__running = False
        self.loop.call_later(self.interval, self.loop.stop)

    def send_message(self, data, addr):
        self.transport.sendto(bencode(data), addr)

    def find_node(self, addr, target=None):
        self.send_message({
            "t": generate_id(),
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.node_id,
                "target": target or generate_node_id()
            }
        }, addr)

    def get_closest_nodes(self, target_id, k_value=8):
        def calc_distance(node):
            return xor(node[0], target_id)

        closest = set()

        r_table_idx = get_routing_table_index(xor(self.node_id, target_id))

        index = r_table_idx
        while index >= 0 and len(closest) < k_value:
            closest |= set(nsmallest(k_value, self.routing_table[index], calc_distance))
            index -= 1

        index = r_table_idx + 1
        while index < 160 and len(closest) < k_value:
            closest |= set(nsmallest(k_value, self.routing_table[index], calc_distance))
            index += 1

        return nsmallest(k_value, closest, calc_distance)

    def add_nodes_to_routing_table(self, nodes):
        new_k = 1500

        for node in nodes:
            r_table_index = get_routing_table_index(xor(node[0], self.node_id))
            rt = self.routing_table[r_table_index]

            if len(rt) < new_k:
                rt.add(node)
            elif get_rand_bool():
                rt.remove(sample(rt, 1)[0])
            else:
                self.find_node((node[1], node[2]))

    def handle_message(self, msg, addr):
        if "y" in msg:
            msg_type = str(msg["y"], "utf-8")

            if msg_type == "r":
                return self.handle_response(msg)

            if msg_type == "q":
                return asyncio.ensure_future(
                    self.handle_query(msg, addr), loop=self.loop
                )

    def handle_response(self, msg):
        args = msg["r"]

        if "nodes" in args:
            nodes = decode_nodes(args["nodes"])
            self.add_nodes_to_routing_table(nodes)

    async def auto_find_nodes(self):
        self.__running = True

        while self.__running:
            await asyncio.sleep(self.interval)

            target_id = generate_node_id()

            for node_id, node_ip, node_port in self.get_closest_nodes(target_id):
                self.find_node((node_ip, node_port), target_id)

    async def handle_query(self, msg, addr):
        args = msg["a"]
        node_id = args["id"]
        query_type = str(msg["q"], "utf-8")

        if query_type == "ping":
            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.node_id
                }
            }, addr)

            await self.ping_received(node_id, addr)

        elif query_type == "find_node":
            target_node_id = args["target"]

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.node_id,
                    "nodes": encode_nodes(self.get_closest_nodes(target_node_id))
                }
            }, addr)

            await self.find_node_received(node_id, target_node_id, addr)

        elif query_type == "get_peers":
            info_hash = args["info_hash"]
            token = generate_node_id()

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.node_id,
                    "nodes": encode_nodes(self.get_closest_nodes(info_hash)),
                    "token": token
                }
            }, addr)

            await self.get_peers_received(node_id, info_hash, addr)

        elif query_type == "announce_peer":
            info_hash = args["info_hash"]
            port = args.get("port", None)

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.node_id
                }
            }, addr)

            await self.announce_peer_received(node_id, info_hash, port, addr)

        await asyncio.sleep(self.interval)
        self.find_node(addr)

    async def ping_received(self, node_id, addr):
        pass

    async def find_node_received(self, node_id, target, addr):
        pass

    async def get_peers_received(self, node_id, info_hash, addr):
        pass

    async def announce_peer_received(self, node_id, info_hash, port, addr):
        pass
