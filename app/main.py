import socket
import threading
import argparse

from .redis_value import RedisValue
from .redis_command import RedisCommand


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379, type=int)
    args = arg_parser.parse_args()

    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    while True:
        sock, ret_addr = server_socket.accept()
        t = threading.Thread(target=lambda: request_handler(sock))
        t.start()


def request_handler(sock: socket.socket) -> None:
    while True:
        request_bytes = sock.recv(1024)
        if not request_bytes:
            continue

        try:
            request_value = RedisValue.from_bytes(request_bytes)
            # print(request_value)
            command = RedisCommand.from_redis_value(request_value)
        except:
            continue

        response_value = command.execute()
        print(response_value.deserialize())
        sock.send(response_value.deserialize())


if __name__ == "__main__":
    main()
