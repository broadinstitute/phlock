import sys
import os
import sqlite3
import __init__ as flock
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging
import threading
import socket
import traceback
import json

log = logging.getLogger("monitor")

__author__ = 'pmontgom'

# schema: SGE job number, job directory, try count, status = STARTED | FAILED | SUCCESS

DB_INIT_STATEMENTS = ["CREATE TABLE TASKS (run_id INTEGER, task_dir STRING primary key, status INTEGER, try_count INTEGER, node_name STRING, external_id STRING, group_number INTEGER )",
 "CREATE INDEX IDX_RUN_ID ON TASKS (run_id)",
 "CREATE INDEX IDX_TASK_DIR ON TASKS (task_dir)",
 "CREATE INDEX IDX_EXTERNAL_ID ON TASKS (external_id)",
 "CREATE INDEX IDX_NODE_NAME ON TASKS (node_name)",
 "CREATE TABLE RUNS (run_id integer primary key autoincrement, run_dir STRING, name STRING, flock_config_path STRING, parameters STRING)",
 "CREATE INDEX IDX_RUN_DIR ON RUNS (run_dir)"]

# Make run_id auto inc primary key
# Make task_dir into primary key
# make status into index

WAITING = 1
READY = 2
SUBMITTED = 3
STARTED = 4
COMPLETED = 5
FAILED = -1
MISSING = -2

status_code_to_name = {WAITING: "WAITING", READY:"READY", SUBMITTED: "SUBMITTED", STARTED: "STARTED", COMPLETED: "COMPLETED", FAILED: "FAILED", MISSING: "MISSING"}

class TransactionContext:
    def __init__(self, connection, lock):
        self.connection = connection
        self.depth = 0
        self.lock = lock

    def __enter__(self):
        if self.depth == 0:
            self.lock.acquire()
        self._db = self.connection.cursor()
        self.depth += 1
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.depth -= 1
        if self.depth == 0:
            self.connection.commit()
            self._db.close()
            self.lock.release()

class TaskStore:
    def __init__(self, db_path):
        new_db = not os.path.exists(db_path)

        self._connection = sqlite3.connect(db_path, check_same_thread=False)
        self._db = self._connection.cursor()
        self._lock = threading.Lock()
        self._active_transaction = None
        self._cv_created = threading.Condition(self._lock)

        if new_db:
            for statement in DB_INIT_STATEMENTS:
                self._db.execute(statement)

    # serialize all access to db via transaction
    def transaction(self):
        if self._active_transaction is None:
            self._active_transaction = TransactionContext(self._connection, self._lock)
        return self._active_transaction

    def get_runs(self):
        with self.transaction() as db:
            db.execute("SELECT run_id, run_dir, name, parameters FROM RUNS")
            rows = db.fetchall()
            result = []
            for run_id, run_dir, name, parameters in rows:
                if parameters != None:
                    parameters = json.loads(parameters)

                db.execute("SELECT status, count(1) FROM TASKS WHERE run_id = ? GROUP BY status", [run_id])
                summary = {}
                for status, count in db.fetchall():
                    summary[status_code_to_name[status]] = count
                result.append(dict(run_dir=run_dir, name=name, parameters=parameters, status=summary))

    def get_version(self):
        return "1"

    def run_created(self, run_id, name, config_path, parameters):
        with self.transaction() as db:
            db.execute("INSERT INTO RUNS (run_dir, name, flock_config_path, parameters) VALUES (?, ?, ?, ?)", [run_id, name, config_path, parameters])
        return True

    def taskset_created(self, run_dir, task_definition_path):
        task_dirs = []
        with open(task_definition_path) as fd:
            for line in fd.readlines():
                line = line.strip()
                if line == "":
                    continue
                group, task_dir = line.split(" ")
                task_dirs.append((int(group), task_dir))

        with self.transaction() as db:
            db.execute("SELECT run_id FROM RUNS WHERE run_dir = ?", [run_dir])
            run_id = db.fetchall()[0][0]

            for group, task_dir in task_dirs:
                if flock.finished_successfully(run_dir, task_dir):
                    status = COMPLETED
                    external_id = None
                else:
                    external_id = flock.get_external_id(run_dir, task_dir)
                    if external_id != None:
                        status = SUBMITTED
                    else:
                        status = WAITING
                db.execute("INSERT INTO TASKS (run_id, task_dir, status, try_count, group_number, external_id) values (?, ?, ?, 0, ?, ?)", [run_id, os.path.join(run_dir, task_dir), status, group, external_id])

            self._cv_created.notify_all()
        return True

    def wait_for_created(self, timeout):
        self._lock.acquire()
        self._cv_created.wait(timeout)
        self._lock.release()

    def task_submitted(self, task_dir, external_id):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ?, external_id = ? WHERE task_dir = ?", [SUBMITTED, external_id, task_dir])
            if db.rowcount == 0:
                log.warn("task_submitted(%s, %s) called, but no record in db", task_dir, external_id)
        return True

    def task_started(self, task_dir, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET try_count = try_count + 1, node_name = ?, status = ? WHERE task_dir = ?", [node_name, STARTED, task_dir])
            if db.rowcount == 0:
                log.warn("task_started(%s, %s) called, but no record in db", task_dir, node_name)
        return True

    def task_failed(self, task_dir):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [FAILED, task_dir])
            if db.rowcount == 0:
                log.warn("task_failed(%s) called, but no record in db", task_dir)
        return True

    def set_task_status(self, task_dir, status):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [status, task_dir])
            if db.rowcount == 0:
                log.warn("set_task_status(%s, %s) called, but no record in db", task_dir, status)
        return True

    def task_missing(self, task_dir):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [MISSING, task_dir])
            if db.rowcount == 0:
                log.warn("task_missing(%s) called, but no record in db", task_dir)
        return True

    def task_completed(self, task_dir):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE task_dir = ?", [COMPLETED, task_dir])
            if db.rowcount == 0:
                log.warn("task_completed(%s) called, but no record in db", task_dir)
        return True

    def node_disappeared(self, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET status = ? WHERE node_name = ?", [WAITING, node_name])
        return True

    def find_tasks_by_status(self, status, limit=None):
        if limit == 0:
            return []

        query = "SELECT run_id, task_dir, group_number FROM tasks WHERE status = ?"
        if limit != None:
            query += " limit %d" % limit
        with self.transaction() as db:
            db.execute(query, [status])
            recs = db.fetchall()
        return recs

    def find_external_ids_of_submitted(self):
        with self.transaction() as db:
            db.execute("SELECT task_dir, external_id FROM tasks WHERE status in (?, ?)", [STARTED, SUBMITTED])
            recs = db.fetchall()
        return recs

    def count_unfinished_tasks_by_group_number(self, run_id):
        result = {}
        with self.transaction() as db:
            db.execute("SELECT group_number, count(*) FROM tasks WHERE status not in (?, ?) AND run_id = ? group by group_number",
                       [FAILED, COMPLETED, run_id])
            for number, count in db.fetchall():
                result[number] = count
        return result

    def get_config_path(self, run_id):
        with self.transaction() as db:
            db.execute("SELECT run_dir, flock_config_path FROM RUNS WHERE run_id = ?", [run_id])
            return db.fetchall()[0]

def identify_tasks_which_disappeared(store, queue):
    # two tasks: 1. identify runs which our db reports as running, but backend queue is no longer reporting as running

    external_id_to_task_dir = dict([(external_id, task_dir) for task_dir, external_id in store.find_external_ids_of_submitted()])

    external_ids_of_those_we_think_are_submitted = set(external_id_to_task_dir.keys())
    external_ids_of_actually_in_queue = set(queue.get_jobs_from_external_queue().keys())

    # identify tasks which transitioned from running -> not running
    # and call these "missing" (assuming the db still claims these are running).  All other transitions
    # should already be performed through other means.

    disappeared_external_ids = external_ids_of_those_we_think_are_submitted - external_ids_of_actually_in_queue
    for external_id in disappeared_external_ids:
        task_dir = external_id_to_task_dir[external_id]

        # check the filesystem to see if it really did succeed and we just missed the notification
        if flock.finished_successfully(None, task_dir):
            store.task_completed(task_dir)
        else:
            store.task_missing(task_dir)

def submit_created_tasks(listener, store, queue_factory, max_submitted=100):
    print "submit_created_tasks"

    submitted_count = len(store.find_tasks_by_status(SUBMITTED))

    # process all the waiting to make sure they've met their requirements
    count_cache = {}
    for run_id, task_dir, group in store.find_tasks_by_status(WAITING):
        if not (run_id in count_cache):
            counts_per_run = store.count_unfinished_tasks_by_group_number(run_id)
            count_cache[run_id] = counts_per_run
        counts = count_cache[run_id]

        # check to make sure that we've completed everything in groups earlier then this one
        all_okay = True
        for other_group, count in counts.items():
            if other_group < group and count > 0:
                all_okay = False

        if all_okay:
            store.set_task_status(task_dir, READY)
        else:
            log.debug("Could not run %s because needs to wait for another job", task_dir)

    # submit any ready tasks
    submit_count = max(0, max_submitted-submitted_count)
    tasks = store.find_tasks_by_status(READY, limit=submit_count)
    queue_cache = {}
    for run_id, task_dir, group in tasks:

        if not (run_id in queue_cache):
            run_dir, config_path = store.get_config_path(run_id)
            config = flock.load_config([config_path], None, {})
            queue = queue_factory(listener, config.qsub_options, config.scatter_qsub_options, config.name, config.workdir)
            queue_cache[run_id] = queue

        queue = queue_cache[run_id]

        queue.submit(run_id, os.path.join(run_dir, task_dir), "scatter" in task_dir)


def main_loop(endpoint_url, flock_home, store, localQueue = False, max_submitted=100):

    if localQueue:
        queue_factory = lambda listener, qsub_options, scatter_qsub_options, name, workdir: flock.LocalBgQueue(listener, workdir)
    else:
        queue_factory = lambda listener, qsub_options, scatter_qsub_options, name, workdir: flock.SGEQueue(listener, qsub_options, scatter_qsub_options, name, workdir)

    listener = flock.ConsolidatedMonitor(endpoint_url, flock_home)
    counter = 0
    t_queue = queue_factory(None, None, None, None, None)
    while True:
        submit_created_tasks(listener, store, queue_factory, max_submitted=max_submitted)
        if counter % 100 == 0:
            identify_tasks_which_disappeared(store, t_queue)
        store.wait_for_created(10)
        counter += 1

def make_function_wrapper(fn):
    def wrapped(*args, **kwargs):
        print "%s(%s, %s)" % (fn.__name__, args, kwargs)
        try:
            return fn(*args, **kwargs)
        except:
            traceback.print_exc()
            raise
    return wrapped

def main():
    FORMAT = "[%(asctime)-15s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO, datefmt="%Y%m%d-%H%M%S")

    db = sys.argv[1]
    port = int(sys.argv[2])
    
    store = TaskStore(db)
    endpoint_url = "http://%s:%d" % (socket.gethostname(), port)
    flock_home = flock.get_flock_home()

    main_loop_thread = threading.Thread(target=lambda: main_loop(endpoint_url, flock_home, store))
    main_loop_thread.daemon = True
    main_loop_thread.start()
    server = SimpleXMLRPCServer(("0.0.0.0", port))
    print "Listening on port %d..." % port
    for method in ["run_created", "taskset_created", "task_submitted", "task_started", "task_failed", "task_completed", "node_disappeared", "get_version", "get_runs"]:
        server.register_function(make_function_wrapper(getattr(store, method)), method)

    server.serve_forever()

if __name__ == "__main__":
    main()
