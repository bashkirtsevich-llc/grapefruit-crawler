import binascii
import math
from collections import namedtuple
from heapq import nsmallest
from secrets import token_bytes, randbits
from socket import inet_ntoa, inet_aton

from chardet import detect

Peer = namedtuple("peer", ["host", "port"])
Node = namedtuple("node", ["id", "host", "port"])


def generate_id():
    return token_bytes(2)


def generate_node_id():
    return token_bytes(20)


def xor(node_one_id, node_two_id):
    return int.from_bytes(node_one_id, "big") ^ int.from_bytes(node_two_id, "big")


def fetch_k_closest_nodes(nodes, target_id, k_value=8):
    return nsmallest(k_value, nodes, lambda node: xor(node.id, target_id))


def get_rand_bool():
    return bool(randbits(1))


def get_routing_table_index(distance):
    if distance:
        return int(math.floor(math.log(math.fabs(distance), 2.0)))
    else:
        return 0


def decode_values(values):
    for value in values:
        if len(value) % 6 != 0:
            return

        ip = inet_ntoa(value[0: 4])  # from network order to IP address
        port = int.from_bytes(value[4: 6], "big")

        if port >= 1024:
            yield Peer(ip, port)


def decode_nodes(nodes):
    if len(nodes) % 26 != 0:
        return

    for i in range(0, len(nodes), 26):
        node_id = nodes[i: i + 20]

        ip = inet_ntoa(nodes[i + 20: i + 24])  # from network order to IP address
        port = int.from_bytes(nodes[i + 24: i + 26], "big")

        if port >= 1024:
            yield Node(node_id, ip, port)


def encode_nodes(nodes):
    result = bytes()

    for node_id, ip, port in nodes:
        ip_message = inet_aton(ip)
        port_message = port.to_bytes(2, "big")

        result = result + node_id + ip_message + port_message

    return result


def hexlify(info_hash):
    return str(binascii.hexlify(info_hash), "utf-8")


def decode_bytes(byte_str):
    if isinstance(byte_str, list):
        return [decode_bytes(item) for item in byte_str]
    if isinstance(byte_str, dict):
        return {key: decode_bytes(value) for key, value in byte_str.items()}
    if isinstance(byte_str, bytes):
        return str(byte_str, detect(byte_str).get("encoding", "utf-8"))
    return byte_str
