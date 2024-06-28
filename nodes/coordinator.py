import asyncio
import concurrent.futures
from simplyrpc import RemoteCallClient
from nodes.node import TwoPhaseCommitNode

class TwoPhaseCommitCoordinator(TwoPhaseCommitNode):

    def __init__(self, log_db_conn, own_hostname, own_port, participants, timeout=10):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.participant_hosts = participants
        self.participants = [] # List of RemoteCallClients
        self.timeout = timeout
        self.current_trans_id = None
        self.exec_counter = 0
        self.batch_size = 3
        self.prepared_to_commit = {} # transaction_id -> [None, None, ...]
        self.done = {} # transaction_id -> [False, False, ...]
        self.everyone_prepared_event = None

    async def setup(self):
        for hostname, port in self.participant_hosts:
            participant = RemoteCallClient(hostname, port)
            participant.timeout = self.timeout
            self.participants.append(participant)
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("DONE", self.recv_done)
        self.server.register_handler("EXECUTE", self.recv_execute)
        self.everyone_prepared_event = asyncio.Event()
        self.initialize_log()

    async def start(self):
        await super().start()
        await self.recover()

    async def send_all(self, kind, data):
        sends = []
        results = []
        for participant in self.participants:
            new_task = asyncio.create_task(participant.send_timeout(kind, data))
            sends.append(new_task)
        for send in sends:
            result = await send
            results.append(result)
        return results

    def set_done(self, trans_id, node_id):
        if trans_id not in self.done:
            self.done[trans_id] = [False] * len(self.participants)
        self.done[trans_id][node_id] = True

    def has_none(self, lst):
        return any([x is None for x in lst])

    def set_prepared(self, trans_id, node_id, prepared=True):
        if trans_id not in self.prepared_to_commit:
            self.prepared_to_commit[trans_id] = [None] * len(self.participants)
        self.prepared_to_commit[trans_id][node_id] = prepared
        if trans_id == self.current_trans_id and not self.has_none(self.prepared_to_commit[trans_id]):
            self.everyone_prepared_event.set()

    async def recv_execute(self, data):
        print("D Received EXECUTE.")
        node_id = int(data["node_id"])
        query = str(data["query"])
        args = tuple(data["args"])
        print(f"Received EXECUTE ({query}) with args {args} for node {node_id} request from client.")
        return await self.execute(node_id, query, args)

    async def execute(self, node_id, query, args):
        print("D Executing.")
        if self.exec_counter == 0:
            began = self.begin_transaction()
            if not began:
                return False
            print("D Began.")
        participant = self.participants[node_id]
        executed = await participant.send_timeout("EXECUTE", (self.current_trans_id, query, args))
        if not executed:
            print("EXECUTE did not reach destination node or was not successful.")
            return False
        print("D Executed.")
        print(f"Sent EXECUTE ({query}) to participant {node_id}.")
        self.exec_counter += 1
        if self.exec_counter == self.batch_size:
            await self.complete_transaction()
        return True

    def begin_transaction(self):
        print("B Begin.")
        if not self.current_trans_id:
            if self.transactions.keys():
                self.current_trans_id = max(self.transactions.keys())
            else:
                self.current_trans_id = 0
        if (self.current_trans_id in self.transactions and self.transactions[self.current_trans_id] not in ["DONE", "PREPARED", "COMMITTED", "ABORTED"]):
            print("May not BEGIN a new transaction; previous one has not completed yet.")
            return False
        self.current_trans_id += 1
        self.everyone_prepared_event.clear()
        return True

    async def complete_transaction(self):
        self.exec_counter = 0
        trans_id = self.current_trans_id
        self.transactions[trans_id] = "PREPARED"
        self.write_log()
        await self.prepare_transaction(trans_id)

    async def prepare_transaction(self, trans_id):
        assert self.transactions[trans_id] == "PREPARED"
        await self.send_all("PREPARE", trans_id)
        print(f"Sent PREPARE {trans_id} to all participants.")
        try:
            await asyncio.wait_for(self.everyone_prepared_event.wait(), self.timeout)
            do_commit = all(self.prepared_to_commit[self.current_trans_id])
        except concurrent.futures.TimeoutError:
            do_commit = False
        if do_commit:
            print(f"Every participant replied with PREPARED")
            self.transactions[trans_id] = "COMMITTED"
            self.write_log()
            await self.commit_transaction(trans_id)
        else:
            print(f"At least one participant replied with PREPARED ABORT or timed out before replying with a PREPARED message.")
            self.transactions[trans_id] = "ABORTED"
            self.write_log()
            await self.abort_transaction(trans_id)

    async def recv_prepare(self, data):
        node_id, trans_id, action = data

        if trans_id not in self.transactions:
            print("PREPARE message for unknown transaction encountered.")
            return False

        state = self.transactions[trans_id]

        if state == "COMMITTED":
            print(f"Received PREPARED from participant {node_id} for transaction that has already committed previously.")
            await self.participants[node_id].send_timeout("COMMIT", trans_id)
            return

        elif state == "ABORTED":
            print(f"Received PREPARED from participant {node_id} for transaction that has already been aborted previously.")
            await self.participants[node_id].send_timeout("ABORT", trans_id)
            return

        elif state == "PREPARED":
            self.set_prepared(trans_id, node_id, (action == "COMMIT"))
            print(f"Received PREPARED {action} from participant {node_id}.")

        else:
            print(f"Illegal PREPARE message received for transaction {trans_id} in state {state} from node {node_id}.")
            return

    async def commit_transaction(self, trans_id):
        assert self.transactions[trans_id] == "COMMITTED"
        print(f"Sending COMMIT to all participants.")
        await self.send_all("COMMIT", trans_id)

    async def abort_transaction(self, trans_id):
        assert self.transactions[trans_id] == "ABORTED"
        print(f"Sending ABORT to all participants.")
        await self.send_all("ABORT", trans_id)

    async def recv_done(self, data):
        node_id, trans_id = data
        if (trans_id in self.transactions and self.transactions[trans_id] not in ["DONE", "COMMITTED", "ABORTED"]):
            print(f"Illegal DONE message received from node {node_id} for transaction {trans_id}.")
            return
        self.set_done(trans_id, node_id)
        print(f"Received DONE from node {node_id}.")
        if all(self.done[trans_id]):
            print(f"Everyone DONE. Removing transaction {trans_id}.")
            self.transactions[trans_id] = "DONE"
            if trans_id in self.done:
                del self.done[trans_id]
            if trans_id in self.prepared_to_commit:
                del self.prepared_to_commit[trans_id]
        return True

    async def recover(self):
        self.read_log()
        print(f"Recovering. Read {len(self.transactions)} transactions from log.")
        tasks = []
        for trans_id, state in self.transactions.items():
            task = None
            if state == "PREPARED":
                task = asyncio.create_task(self.prepare_transaction(trans_id))
            elif state == "COMMITTED":
                task = asyncio.create_task(self.commit_transaction(trans_id))
            elif state == "ABORTED":
                task = asyncio.create_task(self.abort_transaction(trans_id))
            if task:
                tasks.append(task)
        for task in tasks:
            await task