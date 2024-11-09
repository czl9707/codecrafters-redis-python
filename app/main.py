import argparse
import asyncio
import pathlib

from .redis_server import MasterServer, ReplicaServer, ServerConfig, RedisServer


async def main() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379, type=int)
    arg_parser.add_argument("--replicaof", dest="replica_of", default=None, type=str)
    arg_parser.add_argument("--dir", dest="dir", default="/tmp/redis-files", nargs="?")
    arg_parser.add_argument(
        "--dbfilename", dest="dbfilename", default="dump.rdb", nargs="?"
    )
    args = arg_parser.parse_args()

    config: ServerConfig = {
        "dir": pathlib.Path(args.dir),
        "dbfilename": args.dbfilename,
    }

    server: RedisServer
    if args.replica_of:
        url, port = tuple(arg for arg in args.replica_of.split(" ") if len(arg) > 0)
        
        server = ReplicaServer(
            ("localhost", args.port),
            config,
            (url, int(port)),
        )
    else:
        server = MasterServer(("localhost", args.port), config)

    await server.boot()


if __name__ == "__main__":
    asyncio.run(main())
