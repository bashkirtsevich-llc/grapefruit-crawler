import asyncio
from hashlib import sha1

from bencode import bencode, bdecode, decode_dict


class BitTorrentProtocol(asyncio.Protocol):
    def __init__(self, info_hash, result_future):
        self.info_hash = info_hash
        self.result_future = result_future
        self.transport = None
        self.buffer = bytes()
        self.need_handshake = True
        self.metadata = {}

    def connection_made(self, transport):
        self.transport = transport

        data = b"\x13BitTorrent protocol"
        data += b"\x00\x00\x00\x00\x00\x10\x00\x05"
        data += self.info_hash + b"-UT3230-!pT\xed\xb1\xfc\x80x\xa9\x0c\r\x94"
        self.transport.write(data)

    def connection_lost(self, exc):
        if self.transport:
            self.transport.close()
        if not self.result_future.done():
            self.result_future.set_result(None)

    def send_extended_message(self, message_id, message_data):
        buf = b"\x14" + message_id.to_bytes(1, "big") + bencode(message_data)
        self.transport.write(len(buf).to_bytes(4, "big") + buf)

    def handle_message(self, msg_data):
        if msg_data[0] == 0:
            hs_body = bdecode(msg_data[1:])

            metadata_size = hs_body.get("metadata_size", 0)
            ut_metadata_id = hs_body.get("m", {}).get("ut_metadata", 0)

            if metadata_size and ut_metadata_id:
                hs_response = {
                    "e": 0,
                    "metadata_size": metadata_size,
                    "v": "μTorrent 3.2.3",
                    "m": {"ut_metadata": 1},
                    "reqq": 255
                }

                self.send_extended_message(0, hs_response)

                for i in range(0, int(1 + metadata_size / (16 * 1024))):
                    self.send_extended_message(ut_metadata_id, {"msg_type": 0, "piece": i})

        elif msg_data[0] == 1:
            r, l = decode_dict(msg_data[1:], 0)

            if r["msg_type"] == 1:
                self.metadata[r["piece"]] = msg_data[l + 1:]

                metadata = bytes()

                for key in sorted(self.metadata.keys()):
                    metadata += self.metadata[key]

                if len(metadata) == r["total_size"]:
                    if sha1(metadata).digest() == self.info_hash:
                        result = bdecode(metadata)
                    else:
                        result = None

                    if not self.result_future.done():
                        self.result_future.set_result(result)

                    self.transport.close()

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
                msg_len = int.from_bytes(self.buffer[:4], "big")

                if len(self.buffer) >= msg_len + 4:
                    message = parse_message(self.buffer[4: msg_len + 4])
                    if message and message[0] == 20:
                        self.handle_message(message[1])

                    self.buffer = self.buffer[msg_len + 4:]
                else:
                    break
