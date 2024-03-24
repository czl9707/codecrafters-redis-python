import argparse
import asyncio
import pathlib

from .redis_server import MasterServer, ReplicaServer, ServerConfig


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379, type=int)
    arg_parser.add_argument("--replicaof", dest="replica_of", default=[], nargs="*")
    arg_parser.add_argument("--dir", dest="dir", default="/tmp/redis-files", nargs="?")
    arg_parser.add_argument(
        "--dbfilename", dest="dbfilename", default="dump.rdb", nargs="?"
    )
    args = arg_parser.parse_args()

    if len(args.replica_of) not in (0, 2):
        raise Exception("replica of should either have 2 arguments or not been given")

    config: ServerConfig = {
        "dir": pathlib.Path(args.dir),
        "dbfilename": args.dbfilename,
    }

    if args.replica_of:
        server = ReplicaServer(
            ("localhost", args.port),
            config,
            (args.replica_of[0], int(args.replica_of[1])),
        )
    else:
        server = MasterServer(("localhost", args.port), config)

    await server.boot()


if __name__ == "__main__":
    asyncio.run(main())
