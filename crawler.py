import asyncio
from bencode import bencode, bdecode
from random import randint

from utils import generate_node_id, generate_id, get_routing_table_index, xor, decode_nodes, encode_nodes


class DHTCrawler(asyncio.DatagramProtocol):
    def __init__(self, bootstrap_nodes, node_id=None, loop=None, interval=0.001):
        self.node_id = node_id or generate_node_id()
        self.loop = loop or asyncio.get_event_loop()
        self.bootstrap_nodes = bootstrap_nodes
        self.interval = interval

        self.routing_table = [[] for _ in range(160)]

        self.transport = None
        self.__running = False

        self.query_handlers = {
            "ping": self.handle_ping_query,
            "find_node": self.handle_find_nodes_query,
            "get_peers": self.handle_get_peers_query,
            "announce_peer": self.handle_announce_peer_query
        }

    def run(self, port=6881):
        coro = self.loop.create_datagram_endpoint(lambda: self, local_addr=("0.0.0.0", port))
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

    def find_closest_nodes(self, target_id, k_value=8):
        r_table_index = get_routing_table_index(xor(self.node_id, target_id))

        k_closest_nodes = []

        index = r_table_index
        while index >= 0 and len(k_closest_nodes) < k_value:
            for node in self.routing_table[index]:
                if len(k_closest_nodes) < k_value:
                    k_closest_nodes.append(node)
                else:
                    break
            index -= 1

        index = r_table_index + 1
        while index < 160 and len(k_closest_nodes) < k_value:
            for node in self.routing_table[index]:
                if len(k_closest_nodes) < k_value:
                    k_closest_nodes.append(node)
                else:
                    break
            index += 1

        return k_closest_nodes

    def add_nodes_to_routing_table(self, nodes):
        new_k = 1500

        for node in nodes:
            r_table_index = get_routing_table_index(xor(node[0], self.node_id))

            if len(self.routing_table[r_table_index]) < new_k:
                self.routing_table[r_table_index].append(node)
            else:
                if randint(0, 1):
                    index = randint(0, new_k - 1)
                    self.routing_table[r_table_index][index] = node
                else:
                    self.find_node((node[1], node[2]))

    async def auto_find_nodes(self):
        self.__running = True

        while self.__running:
            await asyncio.sleep(self.interval)

            target_id = generate_node_id()

            for node_id, node_ip, node_port in self.find_closest_nodes(target_id):
                self.find_node((node_ip, node_port), target_id)

    def datagram_received(self, data, addr):
        try:
            msg = bdecode(data)
        except:
            return

        try:
            self.handle_message(msg, addr)
        except Exception as e:
            response = {
                "y": "e",
                "e": [202, "Server Error"]
            }

            if "t" in msg:
                response["t"] = msg["t"]

            self.send_message(response, addr)

            raise e

    def handle_message(self, msg, addr):
        msg_type = str(msg.get("y", b"e"), "utf-8")

        if msg_type == "e":
            return

        if msg_type == "r":
            return self.handle_response(msg, addr)

        if msg_type == "q":
            return asyncio.ensure_future(
                self.handle_query(msg, addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        args = msg["r"]

        if "nodes" in args:
            nodes = decode_nodes(args["nodes"])
            self.add_nodes_to_routing_table(nodes)

    async def handle_query(self, msg, addr):
        query_type = str(msg["q"], "utf-8")
        self.query_handlers[query_type](msg, addr)
        self.find_node(addr)

    def handle_ping_query(self, msg, addr):
        self.send_message({
            "t": msg["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        }, addr)

    def handle_find_nodes_query(self, msg, addr):
        target_node_id = msg["a"]["target"]

        self.send_message({
            "t": msg["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "nodes": encode_nodes(self.find_closest_nodes(target_node_id))
            }
        }, addr)

    def handle_get_peers_query(self, msg, addr):
        args = msg["a"]
        info_hash = args["info_hash"]
        token = generate_node_id()

        self.send_message({
            "t": msg["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "nodes": encode_nodes(self.find_closest_nodes(info_hash)),
                "token": token
            }
        }, addr)

        # await self.handle_get_peers(info_hash, addr)
        print("get_peers", info_hash, addr)

    def handle_announce_peer_query(self, msg, addr):
        args = msg["a"]
        info_hash = args["info_hash"]

        self.send_message({
            "t": msg["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        }, addr)

        # await self.handle_announce_peer(info_hash, addr, args.get("port", addr[1]))
        print("announce", info_hash, addr, args.get("port", addr[1]))

    def ping(self, addr):
        self.send_message({
            "y": "q",
            "t": generate_id(),
            "q": "ping",
            "a": {
                "id": self.node_id
            }
        }, addr)

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport.close()
        self.stop()

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
