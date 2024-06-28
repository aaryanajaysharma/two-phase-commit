import asyncio
import psycopg2.errors
import psycopg2.extensions
from simplyrpc import RemoteCallClient
from nodes.node import TwoPhaseCommitNode

class TwoPhaseCommitParticipant(TwoPhaseCommitNode):

    def __init__(self, node_id, data_db_conn, log_db_conn, own_hostname, own_port, coordinator_hostname, coordinator_port, timeout=10):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.node_id = node_id
        self.coordinator = RemoteCallClient(coordinator_hostname, coordinator_port)
        self.timeout = timeout
        self.coordinator.timeout = self.timeout
        self.data_db_conn = data_db_conn
        self.data_db_conn.autocommit = True
        self.data_db_cur = self.data_db_conn.cursor()
        self.server.register_handler("EXECUTE", self.recv_execute)
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("COMMIT", self.recv_commit)
        self.server.register_handler("ABORT", self.recv_abort)
        self.current_trans_id = None

    async def setup(self):
        self.initialize_log()
        with self.data_db_conn:
            self.data_db_cur.execute("create table if not exists data(sensor_id varchar(255) not null primary key, measurement int not null)")

    async def start(self):
        await super().start()
        await self.recover()

    async def stop(self):
        self.data_db_conn.rollback()
        if self.coordinator:
            await self.coordinator.disconnect()
        await super().stop()

    async def begin_transaction(self, trans_id):
        if self.current_trans_id == trans_id:
            return True
        if trans_id in self.transactions:
            if self.transactions[trans_id] != "BEGUN":
                print(f"Trying to append to transaction {trans_id} that is already completed or prepared.")
                return False

        previous_id = self.current_trans_id
        if previous_id in self.transactions:
            if self.transactions[previous_id] == "BEGUN":
                print("Aborting previous transaction because a new one was begun.")
                self.do_abort(previous_id)

        self.current_trans_id = trans_id
        self.transactions[trans_id] = "BEGUN"
        self.data_db_cur.execute("begin")
        print(f"BEGAN new transaction {self.current_trans_id}.")
        return True

    async def recv_execute(self, data):
        trans_id, query, args = data

        if trans_id != self.current_trans_id:
            begun = await self.begin_transaction(trans_id)
            if not begun:
                return False

        print(f"EXECUTE ({query}) for transaction {trans_id} in database.")
        try:
            self.data_db_cur.execute(query, args)
            print("Done.")
        except psycopg2.Error as e:
            self.do_abort(trans_id)
            print(f"EXECUTE failed: {str(e)}")
            return False
        return True

    async def recv_prepare(self, trans_id):
        if trans_id in self.transactions:
            status = self.transactions[trans_id]
            if status == "PREPARED":
                await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "COMMIT"))
                return True
            elif status == "ABORTED":
                await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "ABORT"))
                return True
            elif status == "COMMITTED":
                print("Received invalid PREPARE message; coordinator already told us to commit.")
                return False
            assert status == "BEGUN"

        else:
            begun = await self.begin_transaction(trans_id)
            assert begun

        assert self.current_trans_id == trans_id
        assert self.transactions[trans_id] == "BEGUN"
        try:
            self.data_db_cur.execute("prepare transaction %s", (str(trans_id),))
        except psycopg2.Error as e:
            print(f"PREPARE failed in database.")
            print(str(e))
            self.do_abort(trans_id)
            return False
        self.transactions[trans_id] = "PREPARED"
        self.write_log()
        # if self.node_id == 0:
        #     return False
        await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "COMMIT"))
        print(f"Sent PREPARE COMMIT {trans_id} to coordinator.")
        return True

    async def recv_commit(self, trans_id):
        status = self.transactions[trans_id]
        if status not in ["PREPARED", "COMMITTED"]:
            print(f"Received illegal COMMIT; transaction {trans_id} has state {status}.")
            return False
        self.transactions[trans_id] = "COMMITTED"
        self.write_log()
        try:
            self.data_db_cur.execute("commit prepared %s", (str(trans_id),))
            print(f"COMMITTED {trans_id} into database.")
        except psycopg2.errors.UndefinedObject:
            print(f"Received redundant COMMIT; have already committed this transaction {trans_id}.")
            pass
        except psycopg2.Error as e:
            print("Could not COMMIT!")
            print(str(e))
        await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        print(f"Sent DONE to coordinator.")
        return True

    async def recv_abort(self, trans_id):
        if (trans_id not in self.transactions or
                self.transactions[trans_id] not in ["PREPARED", "ABORTED"]):
            print(f"Received illegal ABORT for transaction {trans_id}.")
            return False
        self.do_abort(trans_id)
        self.write_log()
        await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        print(f"Sent DONE to coordinator.")
        return True

    def do_abort(self, trans_id):
        state = self.transactions.get(trans_id, None)
        if state == "ABORTED":
            print(f"Received redundant ABORT for transaction {trans_id} (in log).")
            return
        if state == "COMMITTED":
            print(f"Cannot abort already COMMITTED transaction {trans_id}.")
            return
        self.transactions[trans_id] = "ABORTED"
        self.write_log()
        try:
            if trans_id == self.current_trans_id:
                self.data_db_cur.execute("abort")
            elif state == "PREPARED":
                self.data_db_cur.execute("rollback prepared %s", (str(trans_id),))
            else:
                assert False
            print(f"ABORTED {trans_id} in database.")
        except psycopg2.errors.UndefinedObject:
            print(f"Received redundant ABORT for transaction {trans_id}.")
            pass
        except psycopg2.Error as e:
            print(f"Could not ABORT!")
            print(e)

    async def recover(self):
        self.read_log()
        awaitables = []
        print(f"Recovering. {len(self.transactions)} transactions read from log.")
        for trans_id, status in self.transactions.items():
            if status == "PREPARED":
                awaitables.append(asyncio.create_task(self.recv_prepare(trans_id)))
            elif status == "COMMITTED":
                awaitables.append(asyncio.create_task(self.recv_commit(trans_id)))
            elif status == "ABORTED":
                awaitables.append(asyncio.create_task(self.recv_abort(trans_id)))
        for awaitable in awaitables:
            await awaitable
