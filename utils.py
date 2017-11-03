import os
import math

from socket import inet_ntoa, inet_aton


def generate_id():
    return os.urandom(2)


def generate_node_id():
    return os.urandom(20)


def get_routing_table_index(distance):
    if distance:
        return int(math.floor(math.log(math.fabs(distance), 2.0)))
    else:
        return 0


def decode_nodes(nodes):
    if len(nodes) % 26 != 0:
        return

    for i in range(0, len(nodes), 26):
        node_id = int.from_bytes(nodes[i: i + 20], byteorder='big')
        ip = inet_ntoa(nodes[i + 20: i + 24])  # from network order to IP address
        port = int.from_bytes(nodes[i + 24: i + 26], byteorder='big')

        yield node_id, ip, port


def encode_nodes(nodes):
    result = bytes()

    for node_id, node_ip, node_port in nodes:
        node_id_bytes = node_id.to_bytes(20, byteorder='big')
        node_ip_bytes = inet_aton(node_ip)
        node_port_bytes = node_port.to_bytes(2, byteorder='big')

        result = result + node_id_bytes + node_ip_bytes + node_port_bytes

    return result
