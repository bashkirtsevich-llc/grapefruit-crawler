import math
import os
from heapq import nsmallest
from random import getrandbits
from socket import inet_ntoa, inet_aton


def generate_id():
    return os.urandom(2)


def generate_node_id():
    return os.urandom(20)


def xor(node_one_id, node_two_id):
    return int.from_bytes(node_one_id, "big") ^ int.from_bytes(node_two_id, "big")


def fetch_k_closest_nodes(nodes, target_id, k_value=8):
    return nsmallest(k_value, nodes, lambda node: xor(node[0], target_id))


def get_rand_bool():
    return bool(getrandbits(1))


def get_routing_table_index(distance):
    if distance:
        return int(math.floor(math.log(math.fabs(distance), 2.0)))
    else:
        return 0


def decode_nodes(nodes):
    if len(nodes) % 26 != 0:
        return

    for i in range(0, len(nodes), 26):
        node_id = nodes[i: i + 20]

        ip = inet_ntoa(nodes[i + 20: i + 24])  # from network order to IP address
        port = int.from_bytes(nodes[i + 24: i + 26], "big")

        yield node_id, ip, port


def encode_nodes(nodes):
    result = bytes()

    for node_id, ip, port in nodes:
        ip_message = inet_aton(ip)
        port_message = port.to_bytes(2, "big")

        result = result + node_id + ip_message + port_message

    return result
