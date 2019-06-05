import asyncio
from aioudp import UDPServer
from collections import namedtuple
from datetime import datetime
from random import SystemRandom

from bencode import bencode, bdecode, BTFailure

from utils import (generate_node_id, generate_id, get_routing_table_index, xor, decode_nodes, encode_nodes,
                   get_rand_bool, fetch_k_closest_nodes, decode_values, Node)

Searcher = namedtuple("searcher", ["info_hash", "nodes", "values", "attempts_count", "timestamp"])


class DHTSpyder(UDPServer):
    def __init__(self, bootstrap_nodes, node_id=None, miner_interval=0.001, **kwargs):
        super(DHTSpyder, self).__init__(**kwargs)

        self.bootstrap_nodes = bootstrap_nodes
        self.node_id = node_id or generate_node_id()
        self.miner_interval = miner_interval

        self.routing_table = [set() for _ in range(160)]
        self.candidates = set()

        self.searchers = {}
        self.searchers_seq = 0

        self.random = SystemRandom()

    def connection_made(self):
        # Bootstrap
        for node in self.bootstrap_nodes:
            self.find_node(node, self.node_id)

        # Start miner
        asyncio.ensure_future(self._dig_periodically(), loop=self.loop)
        asyncio.ensure_future(self._update_searchers_periodically(), loop=self.loop)

    async def datagram_received(self, data, addr):
        try:
            msg = bdecode(data, decoder=lambda t, x: str(x, "utf-8") if t == "key" else x)
        except BTFailure:
            # TODO: Log error
            pass
        else:
            self.handle_message(msg, addr)

    def _fetch_random_candidates(self, k):
        result = self.random.sample(self.candidates, k)

        for item in result:
            self.candidates.remove(item)

        return result

    def _new_event(self, handler):
        asyncio.ensure_future(handler, loop=self.loop)

    async def _dig_periodically(self):
        while True:
            target_id = generate_node_id()

            nodes = [
                *self.get_closest_nodes(target_id),
                *self._fetch_random_candidates(min(len(self.candidates), 7))
            ]

            for _, host, port in nodes:
                self.find_node((host, port), target_id)

            await asyncio.sleep(self.miner_interval)

    async def _update_searchers_periodically(self):
        while True:
            now = datetime.now()
            for t, item in self.searchers.copy().items():
                if (now - item.timestamp).seconds >= 120:
                    if item.values:
                        self._new_event(self.peers_values_received(item.info_hash, item.values))

                    self.searchers.pop(t)

            await asyncio.sleep(1)

    def send_message(self, data, addr):
        self.send(bencode(data), addr)

    def find_node(self, addr, target=None):
        self.send_message({
            "t": generate_id(),
            "y": "q",
            "q": "find_node",
            "a": {
                "id": generate_node_id(),
                "target": target or generate_node_id()
            }
        }, addr)

    def get_peers(self, addr, info_hash, t=None):
        self.send_message({
            "t": t or generate_id(),
            "y": "q",
            "q": "get_peers",
            "a": {
                "id": generate_node_id(),
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

    def add_node(self, node):
        r_table_index = get_routing_table_index(xor(node.id, self.node_id))
        rt = self.routing_table[r_table_index]

        if len(rt) < 1600:
            rt.add(node)
        elif get_rand_bool() and node not in rt:
            rt.remove(self.random.sample(rt, 1)[0])
        else:
            self.find_node((node.host, node.port))

    def search_peers(self, info_hash):
        if self.searchers_seq >= 2 ** 32 - 1:
            self.searchers_seq = 0
        else:
            self.searchers_seq += 1

        t = self.searchers_seq.to_bytes(4, "big")
        self.searchers[t] = Searcher(info_hash, set(), set(), 8, datetime.now())

        for node in self.get_closest_nodes(info_hash, 16):
            self.get_peers((node.host, node.port), info_hash, t)

    def update_peers_searcher(self, t, nodes, values):
        info_hash, old_nodes, old_values, attempts_count, timestamp = self.searchers.pop(t)

        # "nodes" and "values" must be instance of "set" type
        new_nodes = old_nodes | nodes
        new_values = old_values | values

        # If new_closest contains same data as old_closest, we decrease attempt counter
        new_closest, old_closest = (set(fetch_k_closest_nodes(n, info_hash, 16)) for n in (new_nodes, old_nodes))
        if new_closest == old_closest:
            attempts_count -= 1

        if attempts_count > 0:
            self.searchers[t] = Searcher(info_hash, new_nodes, new_values, attempts_count, timestamp)

            for node in new_closest:
                self.get_peers((node.host, node.port), info_hash, t)
        elif new_values:
            self._new_event(self.peers_values_received(info_hash, new_values))

    def handle_message(self, msg, addr):
        try:
            msg_type = str(msg.get("y", b""), "utf-8")

            if msg_type == "r":
                self.handle_response(msg, addr)
            elif msg_type == "q":
                self.handle_query(msg, addr)
        except:
            self.send_message({
                "y": "e",
                "e": [202, "Server Error"],
                "t": msg.get("t", "")
            }, addr)

            raise

    def handle_response(self, msg, addr):
        args = msg["r"]
        t = msg["t"]
        node_id = args["id"]

        nodes = set(decode_nodes(args.get("nodes", b"")))
        values = set(decode_values(args.get("values", [])))

        if t in self.searchers:
            self.update_peers_searcher(t, nodes, values)
        else:
            new_candidates = self.random.sample(nodes, min(len(nodes), 8))

            if len(self.candidates) > 160000:
                self._fetch_random_candidates(len(self.candidates) - 160000)

            self.candidates.update(new_candidates)

        self.add_node(Node(node_id, addr[0], addr[1]))

    def handle_query(self, msg, addr):
        args = msg["a"]
        node_id = args["id"]
        query_type = str(msg["q"], "utf-8")

        response_id = self.node_id

        if query_type == "ping":
            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": response_id
                }
            }, addr)

            self._new_event(self.ping_received(node_id, addr))

        elif query_type == "find_node":
            target_node_id = args["target"]

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": response_id,
                    "nodes": encode_nodes(self.get_closest_nodes(target_node_id))
                }
            }, addr)

            self._new_event(self.find_node_received(node_id, target_node_id, addr))

        elif query_type == "get_peers":
            info_hash = args["info_hash"]
            token = generate_node_id()

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": response_id,
                    "nodes": encode_nodes(self.get_closest_nodes(info_hash)),
                    "token": token
                }
            }, addr)

            self._new_event(self.get_peers_received(node_id, info_hash, addr))

        elif query_type == "announce_peer":
            info_hash = args["info_hash"]
            port = args.get("port", None)

            self.send_message({
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": response_id
                }
            }, addr)

            self._new_event(self.announce_peer_received(node_id, info_hash, port, addr))

        self.add_node(Node(node_id, addr[0], addr[1]))

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
