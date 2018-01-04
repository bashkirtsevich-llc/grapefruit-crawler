import asyncio
from collections import namedtuple
from enum import Enum
from random import randrange
from time import time


class Type(Enum):
    ST_DATA = 0
    ST_FIN = 1
    ST_STATE = 2
    ST_RESET = 3
    ST_SYN = 4


uTPPacket = namedtuple("uTP_packet", [
    "type", "ver", "connection_id", "timestamp", "timestamp_diff", "wnd_size", "seq_nr", "ack_nr", "extensions", "data"
])


def decode_packet(data):
    def _split_bytes(data, lengths):
        idx = 0
        for l in lengths:
            result = data[idx:]
            yield result[:l] if l else result
            idx = idx + l if l else len(data)

    def _bytes_to_int(data):
        return int.from_bytes(data, "big")

    type_ver, next_ext_type, conn_id, tms, tms_diff, wnd, seq, ack, p_data = tuple(
        _split_bytes(data, (1, 1, 2, 4, 4, 4, 2, 2, 0))
    )

    p_type, p_ver = Type(type_ver[0] >> 4), type_ver[0] & 0x0f
    p_conn_id = _bytes_to_int(conn_id)
    p_timestamp = _bytes_to_int(tms)
    p_timestamp_diff = _bytes_to_int(tms_diff)
    p_wnd_size = _bytes_to_int(wnd)
    p_seq_nr = _bytes_to_int(seq)
    p_ack_nr = _bytes_to_int(ack)

    # Decode extensions
    p_extensions = []

    ext_type = next_ext_type[0]
    while ext_type:
        next_ext_type, ext_len, p_data = tuple(_split_bytes(p_data, (1, 1, 0)))
        ext_data, p_data = tuple(_split_bytes(p_data, (int.from_bytes(ext_len, "big"), 0)))
        p_extensions.append((ext_type, ext_data))
        ext_type = next_ext_type[0] if next_ext_type else 0

    return uTPPacket(
        p_type, p_ver, p_conn_id, p_timestamp, p_timestamp_diff, p_wnd_size, p_seq_nr, p_ack_nr, p_extensions, p_data
    )


def encode_packet(packet):
    def _int_to_bytes(data, bytes_len):
        return data.to_bytes(bytes_len, "big")

    result = bytes()

    result += _int_to_bytes(packet.type.value << 4 | packet.ver, 1)
    result += _int_to_bytes(packet.extensions[0][0] if packet.extensions else 0, 1)
    result += _int_to_bytes(packet.connection_id, 2)
    result += _int_to_bytes(packet.timestamp, 4)
    result += _int_to_bytes(packet.timestamp_diff, 4)
    result += _int_to_bytes(packet.wnd_size, 4)
    result += _int_to_bytes(packet.seq_nr, 2)
    result += _int_to_bytes(packet.ack_nr, 2)

    for idx, (_, ext_data) in enumerate(packet.extensions or []):
        result += (
            packet.extensions[idx + 1][0] if idx + 1 < len(packet.extensions) else 0
        ).to_bytes(1, "big")
        result += _int_to_bytes(len(ext_data), 1) + ext_data

    result += packet.data or b""

    return result


def get_tms():
    return int(time() * 10000000) & 0xffffffff


def get_tms_diff():
    return (get_tms() + randrange(10000)) & 0xffffffff


class ConnectionState(Enum):
    CS_UNKNOWN = 1
    CS_SYN_SENT = 2
    CS_SYN_RECV = 3
    CS_CONNECTED = 4
    CS_DISCONNECTED = 5


class MicroTransportProtocol(asyncio.DatagramProtocol):
    def __init__(self, user_protocol):
        self.user_protocol = user_protocol
        self.transport = None
        self.status = ConnectionState.CS_UNKNOWN
        self.seq_nr = 1
        self.ack_nr = None
        self.conn_id_recv = randrange(0xffff)
        self.conn_id_send = self.conn_id_recv + 1
        self.extensions = [(2, bytes(8))]  # 2 -- EXTENSION_BITS

    def connection_made(self, transport):
        self.transport = transport

        packet = uTPPacket(
            type=Type.ST_SYN, ver=1,
            connection_id=self.conn_id_recv,
            timestamp=get_tms(),
            timestamp_diff=0,
            wnd_size=0xf000,
            seq_nr=self.seq_nr,
            ack_nr=0,
            extensions=self.extensions,
            data=None
        )
        self.seq_nr += 1
        self.transport.sendto(encode_packet(packet))
        self.status = ConnectionState.CS_SYN_SENT

    def write(self, data):
        packet = uTPPacket(
            type=Type.ST_DATA, ver=1,
            connection_id=self.conn_id_send,
            timestamp=get_tms(),
            timestamp_diff=get_tms_diff(),
            wnd_size=0xf000,
            seq_nr=self.seq_nr,
            ack_nr=self.ack_nr,
            extensions=None,
            data=data
        )
        self.seq_nr += 1
        self.transport.sendto(encode_packet(packet))

    def datagram_received(self, data, addr):
        packet = decode_packet(data)
        self.ack_nr = packet.seq_nr

        if packet.type == Type.ST_DATA:
            response = uTPPacket(
                type=Type.ST_STATE, ver=1,
                connection_id=self.conn_id_send,
                timestamp=get_tms(),
                timestamp_diff=get_tms_diff(),
                wnd_size=0xf000,
                seq_nr=self.seq_nr,
                ack_nr=self.ack_nr,
                extensions=None,
                data=None
            )
            self.transport.sendto(encode_packet(response))

            if self.status == ConnectionState.CS_SYN_RECV:
                self.status = ConnectionState.CS_CONNECTED
                self.user_protocol.connection_made(self)

            self.user_protocol.data_received(packet.data)
        elif packet.type == Type.ST_SYN:
            self.conn_id_recv = packet.connection_id + 1
            self.conn_id_send = packet.connection_id
            self.seq_nr = randrange(0xffff)
            self.ack_nr = packet.seq_nr
            self.status = ConnectionState.CS_SYN_RECV

            response = uTPPacket(
                type=Type.ST_STATE, ver=1,
                connection_id=self.conn_id_send,
                timestamp=get_tms(),
                timestamp_diff=get_tms_diff(),
                wnd_size=0xf000,
                seq_nr=self.seq_nr,
                ack_nr=self.ack_nr,
                extensions=self.extensions,
                data=None
            )
            self.seq_nr += 1
            self.transport.sendto(encode_packet(response))
        elif packet.type == Type.ST_STATE:
            if self.status == ConnectionState.CS_SYN_SENT:
                self.status = ConnectionState.CS_CONNECTED
                self.user_protocol.connection_made(self)
        elif packet.type == Type.ST_RESET:
            self.status = ConnectionState.CS_DISCONNECTED
            self.user_protocol.connection_lost(None)
        elif packet.type == Type.ST_FIN:
            self.status = ConnectionState.CS_DISCONNECTED
            response = uTPPacket(
                type=Type.ST_FIN, ver=1,
                connection_id=self.conn_id_send,
                timestamp=get_tms(),
                timestamp_diff=get_tms_diff(),
                wnd_size=0xf000,
                seq_nr=self.seq_nr,
                ack_nr=self.ack_nr,
                extensions=None,
                data=None
            )
            self.transport.sendto(encode_packet(response))
            self.user_protocol.connection_lost(None)

    def connection_lost(self, exc):
        self.user_protocol.connection_lost(exc)

    def close(self):
        if self.status == ConnectionState.CS_CONNECTED:
            self.status = ConnectionState.CS_DISCONNECTED
            response = uTPPacket(
                type=Type.ST_FIN, ver=1,
                connection_id=self.conn_id_send,
                timestamp=get_tms(),
                timestamp_diff=get_tms_diff(),
                wnd_size=0xf000,
                seq_nr=self.seq_nr,
                ack_nr=self.ack_nr,
                extensions=None,
                data=None
            )
            self.transport.sendto(encode_packet(response))
            self.user_protocol.connection_lost(None)
