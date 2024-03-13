import socket
import threading

from .redis_value import RedisValue, RedisSimpleString


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        sock, ret_addr = server_socket.accept()
        t = threading.Thread(target=lambda: request_handler(sock))
        t.start()


def request_handler(sock: socket.socket) -> None:
    while True:
        request_bytes = sock.recv(1024)
        print(request_bytes)
        request_value = RedisValue.from_bytes(request_bytes)

        response_value = RedisSimpleString("PONG")

        sock.send(response_value.deserialize())


if __name__ == "__main__":
    main()
