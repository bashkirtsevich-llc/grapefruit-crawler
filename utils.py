import math
import os

import socket
import struct


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


def decode_nodes(message):
    nodes = []
    if len(message) % 26 != 0:
        return nodes

    for i in range(0, len(message), 26):
        node_id = message[i: i + 20]

        try:
            ip = socket.inet_ntoa(message[i + 20: i + 24])  # from network order to IP address
            port = struct.unpack("!H", message[i + 24: i + 26])[0]  # "!" means to read by network order
        except:
            continue

        nodes.append((node_id, ip, port))

    return nodes


def encode_nodes(nodes):
    message = ""
    for node in nodes:
        try:
            ip_message = socket.inet_aton(node[1])
            port_message = struct.pack("!H", node[2])
        except:
            continue  # from IP address to network order
        message = message + node[0] + ip_message + port_message

    return message
