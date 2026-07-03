import socket
import threading
import queue
import os
import sys
import time
import base64
import zlib

from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

import protocol
from chunker import chunk_file, file_checksums

USAGE = """\
Usage:
  python peer.py --role host|client --relay   <relay_host> <relay_port> <sync_dir>
  python peer.py --role host|client --listen  <port> <sync_dir>
  python peer.py --role host|client --connect <host> <port> <sync_dir>

While running, type 'swap' + Enter on either side to reverse sync direction.
"""

conn: socket.socket | None = None
role = 'client'
sync_dir = ''

send_lock = threading.Lock()
swap_lock = threading.Lock()

pending: queue.Queue[str] = queue.Queue()
chunk_needed_replies: queue.Queue[dict] = queue.Queue()
push_cancel = threading.Event()

file_state: dict[str, list[str]] = {}
active_chunks: dict[int, bytes] = {}
active_total = 0

observer = None
push_thread = None


def parse_args():
    global role, sync_dir
    argv = sys.argv[1:]
    if len(argv) < 4 or argv[0] != '--role' or argv[1] not in ('host', 'client'):
        print(USAGE)
        sys.exit(1)
    role = argv[1]
    rest = argv[2:]

    if rest[0] == '--relay' and len(rest) >= 4:
        return {'mode': 'relay', 'host': rest[1], 'port': int(rest[2])}, rest[3]
    if rest[0] == '--listen' and len(rest) >= 3:
        return {'mode': 'listen', 'port': int(rest[1])}, rest[2]
    if rest[0] == '--connect' and len(rest) >= 4:
        return {'mode': 'connect', 'host': rest[1], 'port': int(rest[2])}, rest[3]

    print(USAGE)
    sys.exit(1)


def rel(filepath: str) -> str:
    return os.path.relpath(filepath, sync_dir).replace('\\', '/')


def send(msg: dict):
    with send_lock:
        protocol.send(conn, msg)


# ---------- HOST (push) side ----------

def start_host():
    global observer, push_thread
    push_cancel.clear()
    os.makedirs(sync_dir, exist_ok=True)

    file_state.clear()
    for root, _, files in os.walk(sync_dir):
        for fname in files:
            filepath = os.path.join(root, fname)
            try:
                file_state[rel(filepath)] = file_checksums(filepath)
            except OSError:
                pass

    with pending.mutex:
        pending.queue.clear()
    for root, _, files in os.walk(sync_dir):
        for fname in sorted(files):
            pending.put(rel(os.path.join(root, fname)))

    observer = Observer()
    observer.schedule(SyncWatcher(), sync_dir, recursive=True)
    observer.start()

    push_thread = threading.Thread(target=push_loop, daemon=True)
    push_thread.start()
    print(f"* HOST mode: watching {os.path.abspath(sync_dir)}")


def stop_host():
    global observer, push_thread
    push_cancel.set()
    if observer:
        observer.stop()
        observer.join()
        observer = None
    if push_thread:
        push_thread.join(timeout=2)
        push_thread = None
    with chunk_needed_replies.mutex:
        chunk_needed_replies.queue.clear()


def on_file_changed(filepath: str):
    time.sleep(0.05)
    if role != 'host' or not os.path.isfile(filepath):
        return

    rel_path = rel(filepath)
    try:
        chunks = chunk_file(filepath)
    except OSError:
        return

    new_checksums = [chk for _, chk, _ in chunks]
    old_checksums = file_state.get(rel_path, [])
    changed = sum(
        1 for i, chk in enumerate(new_checksums)
        if i >= len(old_checksums) or old_checksums[i] != chk
    )
    file_state[rel_path] = new_checksums
    if changed == 0:
        return

    print(f"+ {rel_path}: {changed}/{len(new_checksums)} chunks changed")
    pending.put(rel_path)


class SyncWatcher(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory:
            on_file_changed(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            on_file_changed(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            on_file_changed(event.dest_path)


def push_loop():
    while not push_cancel.is_set():
        try:
            rel_path = pending.get(timeout=0.5)
        except queue.Empty:
            continue
        push_file(rel_path)


def push_file(rel_path: str):
    filepath = os.path.join(sync_dir, rel_path)
    if not os.path.isfile(filepath):
        return
    try:
        chunks = chunk_file(filepath)
    except OSError:
        return

    checksums = [chk for _, chk, _ in chunks]
    send({'type': 'CHKSM_DIR', 'file': rel_path, 'checksums': checksums})

    msg = None
    while not push_cancel.is_set():
        try:
            msg = chunk_needed_replies.get(timeout=0.5)
            break
        except queue.Empty:
            continue
    if msg is None or msg.get('file') != rel_path:
        return

    needed = set(msg['chunks'])
    sync_needed = False
    for idx, _chk, data in chunks:
        if push_cancel.is_set():
            return
        if idx in needed:
            compressed = zlib.compress(data)
            sync_needed = True
            send({
                'type': 'CHUNK_DATA', 'file': rel_path, 'index': idx,
                'data': base64.b64encode(compressed).decode(),
            })

    if sync_needed and not push_cancel.is_set():
        send({'type': 'FILE_COMPLETE', 'file': rel_path})
        print(f"  [pushed] {rel_path}  {len(needed)}/{len(chunks)} chunks")


# ---------- CLIENT (pull) side ----------

def start_client():
    global active_chunks, active_total
    os.makedirs(sync_dir, exist_ok=True)
    active_chunks = {}
    active_total = 0
    print(f"* CLIENT mode: receiving into {os.path.abspath(sync_dir)}")


def stop_client():
    global active_chunks, active_total
    active_chunks = {}
    active_total = 0


def write_file(filepath: str, new_chunks: dict[int, bytes], total: int):
    existing: dict[int, bytes] = {}
    try:
        with open(filepath, 'rb') as f:
            i = 0
            while True:
                data = f.read(4096)
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


def handle_client_message(msg: dict):
    global active_chunks, active_total
    t = msg['type']

    if t == 'CHKSM_DIR':
        rel_path = msg['file']
        remote_sums = msg['checksums']
        filepath = os.path.join(sync_dir, rel_path)
        local_sums = file_checksums(filepath)

        needed = [i for i, chk in enumerate(remote_sums) if i >= len(local_sums) or local_sums[i] != chk]
        active_chunks = {}
        active_total = len(remote_sums)
        if needed:
            print(f"  {rel_path}: {len(needed)}/{active_total} chunks needed")
        send({'type': 'CHUNK_NEEDED', 'file': rel_path, 'chunks': needed})

    elif t == 'CHUNK_DATA':
        active_chunks[msg['index']] = zlib.decompress(base64.b64decode(msg['data']))

    elif t == 'FILE_COMPLETE':
        rel_path = msg['file']
        filepath = os.path.join(sync_dir, rel_path)
        write_file(filepath, active_chunks, active_total)
        active_chunks = {}
        print(f"  + {rel_path}")


# ---------- role swap ----------

def do_swap(announce: bool):
    global role
    with swap_lock:
        if announce:
            try:
                send({'type': 'SWAP_ROLE'})
            except OSError:
                pass

        if role == 'host':
            stop_host()
            role = 'client'
            start_client()
        else:
            stop_client()
            role = 'host'
            start_host()

        print(f"* Swapped -> now acting as {role.upper()}")


def stdin_loop():
    while True:
        try:
            line = input()
        except EOFError:
            return
        if line.strip().lower() in ('swap', 's', 'r'):
            do_swap(announce=True)
        elif line.strip():
            print("Type 'swap' + Enter to reverse sync direction.")


# ---------- connection setup ----------

def establish_connection(conn_args: dict) -> socket.socket:
    mode = conn_args['mode']

    if mode == 'relay' or mode == 'connect':
        while True:
            try:
                return socket.create_connection((conn_args['host'], conn_args['port']))
            except OSError as e:
                print(f"! Could not connect ({e}), retrying in 3s...")
                time.sleep(3)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', conn_args['port']))
    server.listen(1)
    print(f"- Listening on port {conn_args['port']}...")
    c, addr = server.accept()
    server.close()
    print(f"+ Peer connected from {addr}")
    return c


def reader_loop():
    while True:
        msg = protocol.recv(conn)
        if msg is None:
            return
        t = msg['type']
        if t == 'SWAP_ROLE':
            do_swap(announce=False)
        elif t == 'CHUNK_NEEDED':
            chunk_needed_replies.put(msg)
        elif t in ('CHKSM_DIR', 'CHUNK_DATA', 'FILE_COMPLETE'):
            handle_client_message(msg)


def main():
    global conn, sync_dir

    conn_args, sd = parse_args()
    sync_dir = sd
    os.makedirs(sync_dir, exist_ok=True)

    threading.Thread(target=stdin_loop, daemon=True).start()
    print("* Type 'swap' + Enter at any time to reverse sync direction")

    while True:
        conn = establish_connection(conn_args)
        print("+ Connected")

        if role == 'host':
            start_host()
        else:
            start_client()

        try:
            reader_loop()
        except KeyboardInterrupt:
            print("\nShutting down!")
            if role == 'host':
                stop_host()
            else:
                stop_client()
            conn.close()
            return

        print("- Connection lost")
        if role == 'host':
            stop_host()
        else:
            stop_client()
        conn.close()

        if conn_args['mode'] != 'relay':
            return
        time.sleep(1)


if __name__ == '__main__':
    main()
