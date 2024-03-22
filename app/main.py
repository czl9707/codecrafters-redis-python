import argparse
import asyncio
import logging

from .redis_server import MasterServer, ReplicaServer


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379, type=int)
    arg_parser.add_argument("--replicaof", dest="replica_of", default=[], nargs="*")
    args = arg_parser.parse_args()

    # logging.basicConfig(filename=f"./logs_{args.port}.txt", level=logging.DEBUG)

    if len(args.replica_of) not in (0, 2):
        raise Exception("replica of should either have 2 arguments or not been given")

    if args.replica_of:
        server = ReplicaServer(
            ("localhost", args.port),
            (args.replica_of[0], int(args.replica_of[1])),
        )
    else:
        server = MasterServer(("localhost", args.port))

    await server.boot()


if __name__ == "__main__":
    asyncio.run(main())
