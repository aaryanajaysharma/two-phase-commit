import asyncio
import argparse

from simplyrpc import RemoteCallClient

def hostname_port_type(inp):
    if ":" in inp:
        hostname, port = inp.split(":")
    else:
        hostname = inp
        port = 12345
    return hostname, int(port)

async def send_execute_request(coordinator, node_id, query, args=tuple()):
    kind = "EXECUTE"
    data = {"node_id": node_id,
            "query": query,
            "args": args}
    print(f"Sending {kind} request to node {node_id}.")
    print(f"Query: {query}")
    print(f"Args: {args}")
    return await coordinator.send(kind, data)

async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--coordinator", type=hostname_port_type, required=True)
    args = argparser.parse_args()
    coordinator_hostname, coordinator_port = args.coordinator
    coordinator = RemoteCallClient(coordinator_hostname, coordinator_port)
    await coordinator.connect()
    print("Connected to coordinator at {}:{}.".format(coordinator_hostname, coordinator_port))
    await recv_input_query(coordinator)

async def recv_input_query(coordinator):
    keep_going = True
    while keep_going:
        print()
        print("------------------- NEW QUERY -------------------", flush=True)
        print()
        print("Execute query:  ", flush=True, end="")
        query = input()
        print("Enter node # : ", flush=True, end="")
        node_id = input()
        success = await send_execute_request(coordinator, node_id, query)
        if not success:
            print("Error: EXECUTE was not successful")
        print("Do you want to send another query request? (y/n): ", flush=True, end="")
        keep_going = None
        while keep_going is None:
            choice = input().lower()
            if(choice == "y"):
                keep_going = True
            elif(choice == "n"):
                keep_going = False

if __name__ == "__main__":
    asyncio.run(main())