import json
import struct

def send(sock, msg: dict):
    data = json.dumps(msg).encode()
    sock.sendall(struct.pack('>I', len(data)) + data)


def recv(sock) -> dict | None:
    raw_len = recv_exact(sock, 4)
    if raw_len is None:
        return None
    length = struct.unpack('>I', raw_len)[0]
    raw_body = recv_exact(sock, length)
    if raw_body is None:
        return None
    return json.loads(raw_body.decode())


def recv_exact(sock, n: int) -> bytes | None:
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf