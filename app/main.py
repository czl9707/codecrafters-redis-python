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
    request_bytes = sock.recv(1024)

    request_value = RedisValue.from_bytes(request_bytes)
    response_count = len(request_value.value[0].value.split("\n"))

    response_value = RedisSimpleString("PONG")

    for _ in range(response_count):
        sock.send(response_value.deserialize())

    # sock.close()


if __name__ == "__main__":
    main()
