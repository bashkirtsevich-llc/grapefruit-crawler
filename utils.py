import os
import math

from socket import inet_ntoa, inet_aton
from struct import unpack, pack


def generate_id():
    return os.urandom(2)


def generate_node_id():
    return os.urandom(20)


def xor(node_one_id, node_two_id):
    result = 0

    for i in range(len(node_one_id)):
        result = (result << 8) + (node_one_id[i] ^ node_two_id[i])

    return result


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

        try:
            ip = inet_ntoa(nodes[i + 20: i + 24])  # from network order to IP address
            port = unpack("!H", nodes[i + 24: i + 26])[0]  # "!" means to read by network order
        except:
            continue

        yield node_id, ip, port


def encode_nodes(nodes):
    result = bytes()

    for node in nodes:
        node_id, ip, port = node
        try:
            ip_message = inet_aton(ip)
            port_message = pack("!H", port)
        except:
            continue  # from IP address to network order

        result = result + node_id + ip_message + port_message

    return result
