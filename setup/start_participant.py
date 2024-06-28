import argparse
import asyncio
import concurrent.futures
import psycopg2
import sys
sys.path.append("..")

from nodes.participant import TwoPhaseCommitParticipant

def hostname_port_type(inp):
    if ":" in inp:
        hostname, port = inp.split(":")
    else:
        hostname = inp
        port = 12345
    return hostname, int(port)

async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--host", type=hostname_port_type, required=True)
    argparser.add_argument("--coordinator", type=hostname_port_type)
    argparser.add_argument("--node-id", type=int)
    argparser.add_argument("--data-db", type=str)
    argparser.add_argument("--log-db", type=str)
    args = argparser.parse_args()
    own_hostname, own_port = args.host
    if args.node_id is None:
        print("All participant nodes must be supplied with a consecutive --node-id starting from zero.")
        return 1
    try:
        log_db = psycopg2.connect(args.log_db)
        data_db = psycopg2.connect(args.data_db)
        coordinator_hostname, coordinator_port = args.coordinator
        the_node = TwoPhaseCommitParticipant(args.node_id, data_db, log_db, own_hostname, own_port, coordinator_hostname, coordinator_port)
        await the_node.setup()
        try:
            await the_node.start()
            print("{} node listening on {}:{}.".format("Participant", own_hostname, own_port))
            while True:
                await asyncio.sleep(0)
        finally:
            await the_node.stop()
            print("Shut down communication node.")
    finally:
        if log_db:
            log_db.close()
            print("Closed log database connection.")
        if data_db:
            data_db.close()
            print("Closed data database connection.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, concurrent.futures.CancelledError):
        print("Killed.")
