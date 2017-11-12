import asyncio
from hashlib import sha1
from struct import pack, unpack
from time import sleep

from bencode import bencode, bdecode, decode_dict

from utils import generate_node_id


class BitTorrentProtocol(asyncio.Protocol):
    def __init__(self, info_hash):
        self.info_hash = info_hash
        self.transport = None
        self.buffer = bytes()
        self.need_handshake = True
        self.metadata = {}

    def connection_made(self, transport):
        transport.write(b"\x13BitTorrent protocol")
        transport.write(b"\x00\x00\x00\x00\x00\x10\x00\x05")
        transport.write(self.info_hash)
        transport.write(generate_node_id())

        self.transport = transport

    def send_extended_message(self, message_id, message_data):
        buf = pack("BB", 20, message_id) + bytes(bencode(message_data), "utf-8")
        self.transport.write(pack("!I", len(buf)) + buf)

    def handle_message(self, msg_data):
        if msg_data[0] == 0:
            hs_body = bdecode(msg_data[1:])

            metadata_size = hs_body.get("metadata_size", 0)
            ut_metadata_id = hs_body.get("m", {}).get("ut_metadata", 0)

            if metadata_size and ut_metadata_id:
                hs_response = {
                    "e": 0,
                    "metadata_size": metadata_size,
                    "v": "uTorrent 3.2.3",
                    "m": {"ut_metadata": 1},
                    "reqq": 255
                }

                self.send_extended_message(0, hs_response)

                sleep(0.05)

                for i in range(0, int(1 + metadata_size / (16 * 1024))):
                    self.send_extended_message(ut_metadata_id, {"msg_type": 0, "piece": i})
                    sleep(0.05)

        elif msg_data[0] == 1:
            r, l = decode_dict(msg_data[1:], 0)

            if r["msg_type"] == 1:
                self.metadata[r["piece"]] = msg_data[l + 1:]

                metadata = bytes()

                for key in sorted(self.metadata.keys()):
                    metadata += self.metadata[key]

                if len(metadata) == r["total_size"] and sha1(metadata).digest() == self.info_hash:
                    print("METADATA!", metadata)

    def data_received(self, data):
        def parse_message(message):
            return (int.from_bytes(message[:1], "big"), message[1:]) if message else None

        self.buffer += data

        if self.need_handshake:
            if len(self.buffer) >= 68:
                self.buffer = self.buffer[68:]
                self.need_handshake = False
        else:
            while len(self.buffer) >= 4:
                msg_len = unpack("!I", self.buffer[:4])[0]

                if len(self.buffer) >= msg_len + 4:
                    message = parse_message(self.buffer[4: msg_len + 4])
                    if message and message[0] == 20:
                        self.handle_message(message[1])

                    self.buffer = self.buffer[msg_len + 4:]
                else:
                    break
