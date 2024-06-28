from simplyrpc import RemoteCallServer

class TwoPhaseCommitNode:
    def __init__(self, log_db_conn, own_hostname, own_port):
        self.server = RemoteCallServer(own_hostname, own_port)
        self.transactions = {} # transaction_id -> status
        self.log_db_conn = log_db_conn # psycopg2.extensions.connection
        self.log_db_cur = self.log_db_conn.cursor() # psycopg2.extensions.cursor

    async def start(self):
        await self.server.start()

    async def stop(self):
        self.write_log()
        await self.server.stop()

    def initialize_log(self):
        self.log_db_cur.execute("create table if not exists log (transaction_id int not null primary key, status varchar(20) not null)")
        self.log_db_conn.commit()
        print("Initialized log table.")

    def write_log(self):
        print("Writing log.")
        with self.log_db_conn:
            self.log_db_cur.execute("select * from log")
            for trans_id, _ in self.log_db_cur.fetchall():
                if trans_id not in self.transactions:
                    self.log_db_cur.execute("delete from log where transaction_id = %s", (trans_id,))
            for trans_id, status in self.transactions.items():
                self.log_db_cur.execute("insert into log (transaction_id, status) values(%s, %s) on conflict (transaction_id) do update set status = %s", (trans_id, status, status))

    def read_log(self):
        self.transactions = {}
        self.log_db_cur.execute("select * from log")
        for trans_id, status in self.log_db_cur:
            self.transactions[trans_id] = status