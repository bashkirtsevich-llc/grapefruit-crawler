import random
from bencode import bencode, bdecode

from utils import generate_node_id, generate_id, get_routing_table_index, xor, decode_nodes, encode_nodes


class DHTProtocol:
    NEW_K = 1500
    TABLE_NUM = 160

    def __init__(self, loop, initial_nodes, node_id=None):
        self.loop = loop
        self.initial_nodes = initial_nodes
        self.node_id = node_id or generate_node_id()

        self.routing_table = [[] for _ in range(DHTProtocol.TABLE_NUM)]

        self.query_handlers = {
            "ping": self.handle_ping_query,
            "find_node": self.handle_find_nodes_query,
            "get_peers": self.handle_get_peers_query,
            "announce_peer": self.handle_announce_peer_query
        }

    def send_message(self, msg, addr):
        self.transport.sendto(bencode(msg), addr)

    def find_node(self, node, target_id=None):
        query = {
            "t": generate_id(),
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.node_id,
                "target": target_id or generate_node_id()
            }
        }

        self.send_message(query, (node[1], node[2]))

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
        for node in nodes:
            r_table_index = get_routing_table_index(xor(node[0], self.node_id))

            if len(self.routing_table[r_table_index]) < DHTProtocol.NEW_K:
                self.routing_table[r_table_index].append(node)
            else:
                if random.randint(0, 1):
                    index = random.randint(0, DHTProtocol.NEW_K - 1)
                    self.routing_table[r_table_index][index] = node
                else:
                    self.find_node(node)

    def bootstrap(self):
        socket_info = self.transport.get_extra_info("sockname")

        self.add_nodes_to_routing_table(
            [(generate_node_id(), node[0], node[1]) for node in self.initial_nodes] +
            [(self.node_id, socket_info[0], socket_info[1])]
        )

    def crawl(self):
        target_id = generate_node_id()

        for node in self.find_closest_nodes(target_id):
            self.find_node(node, target_id)

        self.loop.call_soon_threadsafe(self.crawl)

    def handle(self, msg, addr):
        try:
            msg_type = str(msg["y"], "utf-8")

            if msg_type == "q":
                self.query_handlers[msg["q"]](msg, addr)

            elif msg_type == "r":
                if "nodes" in msg["r"]:
                    self.handle_find_node_response(msg, addr)
        except KeyError:
            pass
        except Exception:
            pass  # Response about error

    def handle_find_node_response(self, data, _):
        node_message = data["r"]["nodes"]
        nodes = decode_nodes(node_message)

        self.add_nodes_to_routing_table(nodes)

    def handle_ping_query(self, data, addr):
        response = {
            "t": data["t"],
            "y": "r",
            "r": {"id": self.node_id}
        }

        self.send_message(response, addr)

    def handle_find_nodes_query(self, data, addr):
        target_node_id = data["a"]["target"]
        r_table_index = get_routing_table_index(xor(self.node_id, target_node_id))

        response_nodes = []
        for node in self.routing_table[r_table_index]:
            if node[0] == target_node_id:
                response_nodes.append(node)
                break

        if len(response_nodes) == 0:
            response_nodes = self.find_closest_nodes(target_node_id)

        node_message = encode_nodes(response_nodes)

        response = {
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "nodes": node_message
            }
        }

        self.send_message(response, addr)

    def handle_get_peers_query(self, data, addr):
        info_hash = data["a"]["info_hash"]

        response = {
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "token": generate_id(),
                "nodes": encode_nodes(self.find_closest_nodes(info_hash))
            }
        }

        self.send_message(response, addr)

        print(info_hash)  # Store to mongodb

    def handle_announce_peer_query(self, data, addr):
        arguments = data["a"]
        info_hash = arguments["info_hash"]
        port = arguments["port"]

        host, _ = addr

        response = {
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        }

        self.send_message(response, addr)

        print(info_hash, addr, port)  # Store to mongodb

    def connection_made(self, transport):
        self.transport = transport

        self.bootstrap()
        self.crawl()

    def datagram_received(self, data, addr):
        self.handle(bdecode(data), addr)
