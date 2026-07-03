import socket
import threading
import queue
import os
import time
import base64
import zlib
import sys

from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

import protocol
from chunker import chunk_file, file_checksums

RELAY_MODE = len(sys.argv) > 1 and sys.argv[1] == '--relay'

if RELAY_MODE:
    RELAY_HOST = sys.argv[2]
    RELAY_PORT = int(sys.argv[3])
    SYNC_DIR = sys.argv[4] if len(sys.argv) > 4 else os.path.join(os.path.dirname(__file__), 'sync')
else:
    PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    SYNC_DIR = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), 'sync')

sessions: list['ClientSession'] = []

file_state: dict[str, list[str]] = {}

def rel(filepath: str) -> str:
    return os.path.relpath(filepath, SYNC_DIR).replace('\\', '/')

class ClientSession(threading.Thread):

    def __init__(self, conn: socket.socket, addr):
        super().__init__(daemon=True)
        self.conn = conn
        self.addr = addr[0]
        self.pending: queue.Queue[str] = queue.Queue()

    def queue_file(self, rel_path: str):
        self.pending.put(rel_path)

    def _sync_file(self, rel_path: str):
        filepath = os.path.join(SYNC_DIR, rel_path)
        if not os.path.isfile(filepath):
            return
        try:
            chunks = chunk_file(filepath)
        except OSError:
            return

        checksums = [chk for _, chk, _ in chunks]

        protocol.send(self.conn, {
            'type': 'CHKSM_DIR',
            'file': rel_path,
            'checksums': checksums,
        })

        msg = protocol.recv(self.conn)
        if not msg or msg['type'] != 'CHUNK_NEEDED':
            return

        needed    = set(msg['chunks'])
        raw_total = sum(len(d) for _, _, d in chunks)
        raw_sent  = sum(len(d) for i, _, d in chunks if i in needed)
        wire_sent = 0
        sync_needed = False

        for idx, _chk, data in chunks:
            if idx in needed:
                compressed = zlib.compress(data)
                wire_sent += len(compressed)
                sync_needed = True
                protocol.send(self.conn, {
                    'type': 'CHUNK_DATA',
                    'file': rel_path,
                    'index': idx,
                    'data': base64.b64encode(compressed).decode(),
                })
                
        if sync_needed:
            protocol.send(self.conn, {'type': 'FILE_COMPLETE', 'file': rel_path})

            delta_saved = raw_total - raw_sent
            comp_saved = raw_sent - wire_sent
            total_saved = raw_total - wire_sent
            pct = (total_saved / raw_total * 100) if raw_total else 0
            print(f"  [{self.addr}] {rel_path}  {len(needed)}/{len(chunks)} chunks"
                f"  |  {wire_sent//1024}KB sent / {raw_total//1024}KB total"
                f"  |  {pct:.0f}% saved  [{delta_saved//1024}KB skipped + {(raw_total - delta_saved)//1024} KB -> {wire_sent//1024} KB compressed]")

    def run(self):
        print(f"+ {self.addr} connected")
        sessions.append(self)

        for root, _, files in os.walk(SYNC_DIR):
            for fname in sorted(files):
                self.pending.put(rel(os.path.join(root, fname)))

        try:
            while True:
                try:
                    rel_path = self.pending.get(timeout=0.5)
                    self._sync_file(rel_path)
                except queue.Empty:
                    pass
        except Exception as e:
            print(f"[-] {self.addr} error: {e}")
        finally:
            if self in sessions:
                sessions.remove(self)
            self.conn.close()
            print(f"[-] {self.addr} disconnected")


def on_file_changed(filepath: str):
    time.sleep(0.05)
    if not os.path.isfile(filepath):
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
        if i >= len(old_checksums) 
        or old_checksums[i] != chk
    )
    file_state[rel_path] = new_checksums

    if changed == 0:
        return

    print(f"+ {rel_path}: {changed}/{len(new_checksums)} chunks changed")
    for session in sessions:
        session.queue_file(rel_path)


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

def main():
    os.makedirs(SYNC_DIR, exist_ok=True)

    for root, _, files in os.walk(SYNC_DIR):
        for fname in files:
            filepath = os.path.join(root, fname)
            rel_path = rel(filepath)
            try:
                file_state[rel_path] = file_checksums(filepath)
            except OSError:
                pass

    observer = Observer()
    observer.schedule(SyncWatcher(), SYNC_DIR, recursive=True)
    observer.start()

    print(f"- Watching: {os.path.abspath(SYNC_DIR)}")
    print(f"- Press Ctrl+C to stop")

    try:
        if RELAY_MODE:
            print(f"- Dialing relay at {RELAY_HOST}:{RELAY_PORT}")
            while True:
                try:
                    conn = socket.create_connection((RELAY_HOST, RELAY_PORT))
                except OSError as e:
                    print(f"! Could not reach relay ({e}), retrying in 3s...")
                    time.sleep(3)
                    continue
                print("+ Connected to relay")
                session = ClientSession(conn, (RELAY_HOST, RELAY_PORT))
                session.start()
                session.join()
                print("- Relay connection lost, reconnecting...")
        else:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('0.0.0.0', PORT))
            server.listen(10)
            server.settimeout(1.0)
            print(f"- DeltaSync host listening on port {PORT}")
            while True:
                try:
                    conn, addr = server.accept()
                    ClientSession(conn, addr).start()
                except socket.timeout:
                    pass
    except KeyboardInterrupt:
        print("\nShutting down!")
    finally:
        observer.stop()
        observer.join()

if __name__ == '__main__':
    main()