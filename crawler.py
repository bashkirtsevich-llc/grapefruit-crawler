import asyncio
from random import sample

from bencode import bencode, bdecode

from utils import (generate_node_id, generate_id, get_routing_table_index, xor, decode_nodes, encode_nodes,
                   get_rand_bool, fetch_k_closest_nodes, decode_values)


class DHTCrawler(asyncio.DatagramProtocol):
    def __init__(self, bootstrap_nodes, node_id=None, loop=None, interval=0.001):
        self.node_id = node_id or generate_node_id()
        self.loop = loop or asyncio.get_event_loop()
        self.bootstrap_nodes = bootstrap_nodes
        self.interval = interval

        self.routing_table = [set() for _ in range(160)]

        self.searchers = {}
        self.searchers_seq = 0

        self.transport = None
        self.__running = False

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.stop()
        self.transport.close()

    def datagram_received(self, data, addr):
        try:
            msg = bdecode(data)
            asyncio.ensure_future(self.handle_message(msg, addr), loop=self.loop)
        except:
            pass

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

    def get_peers(self, addr, info_hash, t=None):
        self.send_message({
            "t": t or generate_id(),
            "y": "q",
            "q": "get_peers",
            "a": {
                "id": self.node_id,
                "info_hash": info_hash
            }
        }, addr)

    def get_closest_nodes(self, target_id, k_value=8):
        closest_l = set()
        closest_r = set()

        r_table_idx = get_routing_table_index(xor(self.node_id, target_id))

        idx = r_table_idx
        while idx >= 0 and len(closest_l) < k_value:
            closest_l |= set(fetch_k_closest_nodes(self.routing_table[idx], target_id, k_value))
            idx -= 1

        idx = r_table_idx + 1
        while idx < 160 and len(closest_r) < k_value:
            closest_r |= set(fetch_k_closest_nodes(self.routing_table[idx], target_id, k_value))
            idx += 1

        return fetch_k_closest_nodes(closest_l | closest_r, target_id, k_value)

    def add_nodes_to_routing_table(self, nodes):
        new_k = 1500

        for node in nodes:
            r_table_index = get_routing_table_index(xor(node[0], self.node_id))
            rt = self.routing_table[r_table_index]

            if len(rt) < new_k:
                rt.add(node)
            elif get_rand_bool() and node not in rt:
                rt.remove(sample(rt, 1)[0])
            else:
                self.find_node((node[1], node[2]))

            self.routing_table[r_table_index] = rt  # ???

    async def search_peers(self, info_hash):
        self.searchers_seq += 1

        t = self.searchers_seq.to_bytes(4, "big")
        self.searchers[t] = (info_hash, set(), set(), 8)  # (info_hash, nodes, values, attempts_count)

        for node in self.get_closest_nodes(info_hash):
            self.get_peers((node[1], node[2]), info_hash, t)

    async def update_peers_searcher(self, t, nodes, values):
        searcher = self.searchers.pop(t, None)
        if not searcher:
            return

        info_hash, old_nodes, old_values, attempts_count = searcher

        # "nodes" and "values" must be instance of "set" type
        new_nodes = old_nodes | nodes
        new_values = old_values | values

        # If new_closest contains same data as old_closest, we decrease attempt counter
        new_closest, old_closest = (set(fetch_k_closest_nodes(n, info_hash)) for n in (new_nodes, old_nodes))
        if new_closest == old_closest:
            attempts_count -= 1

        if attempts_count > 0:
            self.searchers[t] = (info_hash, new_nodes, new_values, attempts_count)

            for node in new_closest:
                self.get_peers((node[1], node[2]), info_hash, t)
        elif new_values:
            await self.peers_values_received(info_hash, new_values)

    async def handle_message(self, msg, addr):
        try:
            msg_type = str(msg.get("y", b""), "utf-8")

            if msg_type == "r":
                await self.handle_response(msg)
            elif msg_type == "q":
                await self.handle_query(msg, addr)
        except:
            self.send_message({
                "y": "e",
                "e": [202, "Server Error"],
                "t": msg.get("t", "")
            }, addr)

            raise

    async def handle_response(self, msg):
        args = msg["r"]

        nodes = set(decode_nodes(args.get("nodes", b"")))
        values = set(decode_values(args.get("values", [])))

        # TODO: Add delay before start
        # TODO: May be it should exec as "ensure_future()"?
        await self.update_peers_searcher(msg["t"], nodes, values)
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

    async def peers_values_received(self, info_hash, peers):
        pass
