import argparse

from .redis_cache import RedisCache


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379, type=int)
    arg_parser.add_argument("--replicaof", dest="replica_of", default=[], nargs="*")
    args = arg_parser.parse_args()

    if len(args.replica_of) not in (0, 2):
        raise Exception("replica of should either have 2 arguments or not been given")

    cache = RedisCache()
    if args.replica_of:
        cache.config(
            is_master=False,
            master_addr=(args.replica_of[0], int(args.replica_of[1])),
        )
    cache.boot(("localhost", args.port))


if __name__ == "__main__":
    main()
