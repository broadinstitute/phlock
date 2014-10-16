import os
import sqlite3
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer

__author__ = 'pmontgom'

# schema: SGE job number, job directory, try count, status = STARTED | FAILED | SUCCESS

DB_INIT_STATEMENTS = ["CREATE TABLE TASKS (run_id STRING, task_dir STRING, status INTEGER, try_count INTEGER, node_name STRING, external_id STRING)",
 "CREATE INDEX IDX_RUN_ID ON TASKS (run_id)",
 "CREATE INDEX IDX_TASK_DIR ON TASKS (task_dir)",
 "CREATE INDEX IDX_NODE_NAME ON TASKS (node_name)"]

STARTED = 1
FAILED = -1
SUCCESS = 2
SUBMITTED = 20
READY = 100


class TransactionContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self._db = self.connection.cursor()
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self._db.close()

class TaskStore:
    def __init__(self, db_path):
        new_db = not os.path.exists(db_path)

        self._connection = sqlite3.connect(db_path)
        self._db = self._connection.cursor()

        if new_db:
            for statement in DB_INIT_STATEMENTS:
                self._db.execute(statement)

    def transaction(self):
        return TransactionContext(self._connection)

    def get_version(self):
        return "1"

    def task_created(self, run_id, task_dir, external_id):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ?, external_id = ? WHERE task_dir = ?", [SUBMITTED, external_id, task_dir])
            if db.rowcount == 0:
                db.execute("INSERT INTO TASKS (run_id, task_dir, status, try_count, external_id) values (?, ?, ?, 1, ?)", [run_id, task_dir, SUBMITTED, external_id])
        return True

    def task_started(self, run_id, task_dir, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET try_count = try_count + 1, status = ? WHERE task_dir = ?", [STARTED, task_dir])
            if db.rowcount == 0:
                db.execute("INSERT INTO TASKS (run_id, task_dir, status, try_count, node_name) values (?, ?, ?, 1, ?)", [run_id, task_dir, STARTED, node_name])
        return True

    def task_failed(self, run_id, task_dir):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [FAILED, task_dir])
            if db.rowcount == 0:
                db.execute("INSERT INTO TASKS (run_id, task_dir, status, try_count, node_name) values (?, ?, ?, 1, NULL)", [run_id, task_dir, FAILED])
        return True

    def task_completed(self, run_id, task_dir):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [SUCCESS, task_dir])
        return True

    def node_disappeared(self, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE node_name = ?", [READY, node_name])
        return True

def main(db, port):
    store = TaskStore(db)

    server = SimpleXMLRPCServer(("0.0.0.0", port))
    print "Listening on port %d..." % port
    for method in ["task_created", "task_started", "task_failed", "task_completed", "node_disappeared", "get_version"]:
        server.register_function(getattr(store, method), method)

    server.serve_forever()

if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))