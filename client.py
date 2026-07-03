import socket
import os
import sys
import base64
import hashlib
import zlib

import protocol
from chunker import CHUNK_SIZE

HOST = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
SYNC_DIR = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(__file__), 'received')

def local_checksums(filepath: str) -> list[str]:
    sums = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                sums.append(hashlib.md5(data).hexdigest())
    except FileNotFoundError:
        pass
    return sums


def write_file(filepath: str, new_chunks: dict[int, bytes], total: int):
    existing: dict[int, bytes] = {}
    try:
        with open(filepath, 'rb') as f:
            i = 0
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                existing[i] = data
                i += 1
    except FileNotFoundError:
        pass

    existing.update(new_chunks)

    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, 'wb') as f:
        for i in range(total):
            if i in existing:
                f.write(existing[i])

def run():
    os.makedirs(SYNC_DIR, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print(f"+ Connected to {HOST}:{PORT}")
    print(f"+ Syncing to: {os.path.abspath(SYNC_DIR)}")

    active_chunks: dict[int, bytes] = {}
    active_total  = 0

    while True:
        msg = protocol.recv(sock)
        if msg is None:
            print("- Host closed the connection")
            break

        t = msg['type']

        if t == 'CHKSM_DIR':
            rel_path = msg['file']
            remote_sums = msg['checksums']
            filepath = os.path.join(SYNC_DIR, rel_path)
            local_sums = local_checksums(filepath)

            needed = [i for i, chk in enumerate(remote_sums) if i >= len(local_sums) or local_sums[i] != chk]

            active_chunks = {}
            active_total = len(remote_sums)
            
            if needed:
                print(f"  {rel_path}: {len(needed)}/{active_total} chunks needed")
            protocol.send(sock, {'type': 'CHUNK_NEEDED', 'file': rel_path, 'chunks': needed})

        elif t == 'CHUNK_DATA':
            active_chunks[msg['index']] = zlib.decompress(base64.b64decode(msg['data']))

        elif t == 'FILE_COMPLETE':
            rel_path = msg['file']
            filepath = os.path.join(SYNC_DIR, rel_path)
            write_file(filepath, active_chunks, active_total)
            active_chunks = {}
            print(f"  + {rel_path}")

    sock.close()

if __name__ == '__main__':
    run()
