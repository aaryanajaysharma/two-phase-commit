# Two-phase Commit Server Implementation

An implementation of [two-phase commit protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol) 
in Python for a project in the course CS3.401: Distributed Systems, offered at IIIT-Hyderabad. We assume the network to be non-byzantine, but prone to failure.

## Installation

### Requirements

* `psycopg2`: Python Interface to PostgreSQL
* `simplyrpc`: Simple RPC implementation in python

Install using `requirements.txt`

`pip install -r requirements.txt`

PostgreSQL must be installed on the system for the databases.
For `N` participants, we would need `2*N+1` independent PostgreSQL server instances to act as
seperate nodes. To spin up these databases automatically, `cd` into `setup` and run:

    python3 setup_db.py {2*N+1}

For example, if the coordinator is chosen to run at port `16000` and the `i`th participant at `16000+i`, then 
* Database ports
    * For coordinator `logdb`: `15001` (say)
    * For participant `i`, `logdb`: `15000+(2*i)`
    * For participant `i`, `datadb`: `15000+2*i+1`

## Running the nodes
- `cd` into `./setup`
Start the coordinator:
`python3 start_coordinator.py 
        --host localhost:16000 
        --participant localhost:16001 
        --participant localhost:16002 
        --log-db postgresql://:15001 
        --batch-size 3 --timeout 3`

Start a participant
- `cd` into `./setup`
`python3 main.py 
--node-id 0 
--host localhost:16001 
--log-db 
postgresql://:15002 
--data-db postgresql://:15003 
--coordinator localhost:16000`

Start a client from the root directory to send insert requests to the coordinator:

    python3 client.py --coordinator localhost:16000


## Project Deliverables Status

- [x] Logging functionality
- [x] Data storage functionality
- [x] Dynamic query execution of client
- [x] Implementation of two-phase commit protocol
- [x] Handling of upto one node failure

## Outline of code

`start_coordinator.py`: Runs the Coordinator node. It uses asyncio for asynchronous execution and argparse for parsing command-line arguments. The coordinator communicates with participant nodes, coordinates their actions, and manages transaction logs stored in a PostgreSQL database. Listens for commands, sets up the coordinator node, and handles shutdown gracefully.

`start_participant.py`: Runs the Participant node. Employs asyncio for asynchronous execution and argparse for parsing command-line arguments. The participant communicates with the coordinator node, manages its own data and transaction logs stored in PostgreSQL databases, and handles graceful shutdown upon receiving termination signals.

`coordinator.py`: Implements a two-phase commit protocol coordinator, managing transaction processes among various nodes. It coordinates the actions of participants, ensuring all nodes reach a consensus on commit or abort decisions before executing transactions.

`node.py`: Defines the base functionality for a node within a two-phase commit system, supporting basic network operations and transaction state management. This includes functionalities like writing to the logs, setting up basic RPC client/server etc.

`participant.py`: Represents a participant node in a two-phase commit system, handling specific commands such as prepare, commit, and abort from the coordinator. It ensures that actions on the database are executed according to the coordinator’s directives and maintains logs for transaction recovery.

`client.py`: Provides interface to dynamically send requests to the system and execute queries.
