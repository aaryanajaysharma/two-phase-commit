import argparse
import asyncio
import concurrent.futures
import psycopg2
import sys

sys.path.append("..")

from nodes.coordinator import TwoPhaseCommitCoordinator

def hostname_port_type(inp):
    if ":" in inp:
        hostname, port = inp.split(":")
    else:
        hostname = inp
        port = 12345
    return hostname, int(port)

async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--host", type=hostname_port_type, required=True,)
    argparser.add_argument("--participant", type=hostname_port_type, action="append")
    argparser.add_argument("--log-db", type=str)
    argparser.add_argument("--timeout", type=int, default=10)
    argparser.add_argument("--batch-size", type=int)
    args = argparser.parse_args()
    own_hostname, own_port = args.host
    try:
        log_db = psycopg2.connect(args.log_db)
        the_node = TwoPhaseCommitCoordinator(log_db, own_hostname, own_port, args.participant, timeout=args.timeout)
        if args.batch_size:
                the_node.batch_size = args.batch_size
        await the_node.setup()
        try:
            await the_node.start()
            print("{} node listening on {}:{}.".format("Coordinator", own_hostname, own_port))
            while True:
                await asyncio.sleep(0)
        finally:
            await the_node.stop()
            print("Shut down communication node.")
    finally:
        if log_db:
            log_db.close()
            print("Closed log database connection.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, concurrent.futures.CancelledError):
        print("Killed.")
