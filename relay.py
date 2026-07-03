import socket
import threading
import sys

HOST_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
CLIENT_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8000


def pipe(src: socket.socket, dst: socket.socket):
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def splice(a: socket.socket, b: socket.socket):
    t1 = threading.Thread(target=pipe, args=(a, b), daemon=True)
    t2 = threading.Thread(target=pipe, args=(b, a), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    a.close()
    b.close()


def main():
    host_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    host_listener.bind(('0.0.0.0', HOST_PORT))
    host_listener.listen(1)

    client_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_listener.bind(('0.0.0.0', CLIENT_PORT))
    client_listener.listen(5)

    print(f"- Relay up: host dials in on {HOST_PORT}, clients dial in on {CLIENT_PORT}")

    try:
        while True:
            print("- Waiting for host to connect...")
            host_conn, host_addr = host_listener.accept()
            print(f"+ Host connected from {host_addr}")

            print("- Waiting for a client to connect...")
            client_conn, client_addr = client_listener.accept()
            print(f"+ Client connected from {client_addr}")

            splice(host_conn, client_conn)
            print("- Pair disconnected, resetting relay")
    except KeyboardInterrupt:
        print("\nShutting down!")
    finally:
        host_listener.close()
        client_listener.close()


if __name__ == '__main__':
    main()
