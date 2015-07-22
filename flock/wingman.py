import os
import sqlite3
import __init__ as flock
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
import logging
import threading
import socket
import traceback
import json
import wingman_client
from queue.sge import SGEQueue
from queue.local import LocalBgQueue
import config as flock_config
import time
import glob
import base64
import hashlib
import collections
import wingman_sge_stat
import shutil
import trace_on_demand

log = logging.getLogger("monitor")

__author__ = 'pmontgom'

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
KILLED = -3
KILL_PENDING = -4
KILL_SUBMITTED = -5
PREREQ_FAILED = -6
QUOTA_EXCEEDED = -7

# classes of states:
#   successful completion: COMPLETED
#   failed completion: KILLED, PREREQ_FAILED, FAILED, QUOTA_EXCEEDED
#   waiting: WAITING
#   in-progress: READY, SUBMITTED, STARTED, MISSING, KILL_PENDING, KILL_SUBMITTED

status_code_to_name = {WAITING: "WAITING", READY:"READY", SUBMITTED: "SUBMITTED", STARTED: "STARTED",
                       COMPLETED: "COMPLETED", FAILED: "FAILED", MISSING: "MISSING", KILLED: "KILLED",
                       KILL_PENDING : "KILL_PENDING", KILL_SUBMITTED: "KILL_SUBMITTED", PREREQ_FAILED: "PREREQ_FAILED",
                       QUOTA_EXCEEDED: "QUOTA_EXCEEDED"}


MIGRATIONS = [
    ("000-initial-schema", [
        "CREATE TABLE TASKS (run_id INTEGER, task_dir STRING primary key, status INTEGER, try_count INTEGER, node_name STRING, external_id STRING, group_number INTEGER )",
        "CREATE INDEX IDX_RUN_ID ON TASKS (run_id)",
        "CREATE INDEX IDX_TASK_DIR ON TASKS (task_dir)",
        "CREATE INDEX IDX_EXTERNAL_ID ON TASKS (external_id)",
        "CREATE INDEX IDX_NODE_NAME ON TASKS (node_name)",
        "CREATE TABLE RUNS (run_id integer primary key autoincrement, run_dir STRING UNIQUE, name STRING, flock_config_path STRING, parameters STRING, required_mem_override INTEGER)",
        "CREATE INDEX IDX_RUN_DIR ON RUNS (run_dir)"]),
    ("001-create-migration-table", ["CREATE TABLE SCHEMA_MIGRATION (name string primary key)"]),
    ("001-add-run-columns", [
        "ALTER TABLE RUNS RENAME TO OLD_RUNS_20150505",
        "CREATE TABLE RUNS (run_id integer primary key autoincrement, run_dir STRING UNIQUE, name STRING, flock_config_path STRING, parameters STRING, added_tags STRING, required_mem_override INTEGER, archive_name STRING)",
        "INSERT INTO RUNS (run_id, run_dir, name, flock_config_path, parameters, required_mem_override) select run_id, run_dir, name, flock_config_path, parameters, required_mem_override FROM OLD_RUNS_20150505",
        "DROP TABLE OLD_RUNS_20150505",
        "CREATE INDEX IDX_RUN_DIR ON RUNS (run_dir)",
        "CREATE INDEX IDX_RUN_ARCHIVE ON RUNS (archive_name)"]),
    ("002-add-update-time-on-task", [
        "ALTER TABLE TASKS RENAME TO OLD_TASKS_20150520",
        "CREATE TABLE TASKS (run_id INTEGER, task_dir STRING primary key, status INTEGER, try_count INTEGER, node_name STRING, external_id STRING, group_number INTEGER, update_time NUMBER )",
        "INSERT INTO TASKS (run_id, task_dir, status, try_count, node_name, external_id, group_number) SELECT run_id, task_dir, status, try_count, node_name, external_id, group_number FROM OLD_TASKS_20150520",
        "UPDATE TASKS SET update_time = %f" % time.time(),
        "DROP TABLE OLD_TASKS_20150520",
        "CREATE INDEX IDX_RUN_ID ON TASKS (run_id)",
        "CREATE INDEX IDX_TASK_DIR ON TASKS (task_dir)",
        "CREATE INDEX IDX_EXTERNAL_ID ON TASKS (external_id)",
        "CREATE INDEX IDX_NODE_NAME ON TASKS (node_name)"])
]

def upgrade_db(db_path):
    new_db = not os.path.exists(db_path)
    connection = sqlite3.connect(db_path)
    db = connection.cursor()

    # determine which migrations have been applied
    applied_migrations = set()
    if not new_db:
        applied_migrations.add("000-initial-schema")

    db.execute("PRAGMA table_info(SCHEMA_MIGRATION)")
    if len(db.fetchall()) > 0:
        # if the table exists query the applied migrations
        db.execute("SELECT name FROM SCHEMA_MIGRATION");
        applied_migrations.update( [x[0] for x in db.fetchall()] )

    # for each missing migration, get the statements to execute
    statements_to_execute = []
    for name, statements in MIGRATIONS:
        if not (name in applied_migrations):
            log.warn("Need to apply sql migration %s", name)
            statements_to_execute.extend(statements)
            if name != "000-initial-schema":
                statements_to_execute.append("INSERT INTO SCHEMA_MIGRATION VALUES ('%s')" % name)
    db.close()
    connection.close()

    # If we have statements to execute, make a backup of the db before applying
    if len(statements_to_execute) > 0:
        timestamp = str(time.time())
        db_path_backup = db_path+timestamp+".backup"
        db_path_temp = db_path+timestamp+".migrate"
        log.warn("Upgrading schema... ( %s -> %s )", db_path, db_path_temp)
        try:
            shutil.copy(db_path, db_path_temp)
            connection = sqlite3.connect(db_path_temp)
            db = connection.cursor()

            for statement in statements_to_execute:
                try:
                    db.execute(statement)
                except:
                    print("failed:", statement)
                    raise

            connection.commit()
            db.close()
            connection.close()
            log.warn("Renaming %s -> %s", db_path, db_path_backup)
            os.rename(db_path, db_path_backup)
            log.warn("Renaming %s -> %s", db_path_temp, db_path)
            os.rename(db_path_temp, db_path)

        except:
            print("Migration failed.  DB was being written to "+db_path_temp)
            raise
        log.warn("DB upgrade complete")


MONITOR_POLL_INTERVAL = 60
def format_watch_command(flock_home, log_file):
    return "python %s/watch_proc.py %s %d" % (flock_home, log_file, MONITOR_POLL_INTERVAL)

def format_notify_command(flock_home, endpoint_url):
    return "python %s/wingman_notify.py %s" % (flock_home, endpoint_url)

THREAD_LOCALS = threading.local()

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
            if exc_type == None:
                self.connection.commit()
            else:
                self.connection.rollback()
            self._db.close()
            self.lock.release()
            THREAD_LOCALS._active_transaction = None

class TaskStore:
    def __init__(self, db_path, flock_home, endpoint_url):
        self.flock_home = flock_home
        self.endpoint_url = endpoint_url
        self.volume_history = wingman_sge_stat.VolumeHistory()

        upgrade_db(db_path)

        self._archive_path = os.path.join(os.path.dirname(db_path), "archived")
        self._volumes_to_watch = [os.path.dirname(db_path)]
        self._connection = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        THREAD_LOCALS._active_transaction = None
        self._cv_created = threading.Condition(self._lock)

    # serialize all access to db via transaction
    def transaction(self):
        if (not hasattr(THREAD_LOCALS, "_active_transaction")) or THREAD_LOCALS._active_transaction is None:
            THREAD_LOCALS._active_transaction = TransactionContext(self._connection, self._lock)
        return THREAD_LOCALS._active_transaction

    def update_volume_history(self):
        for volume in self._volumes_to_watch:
            self.volume_history.update(volume)

    def get_host_summary(self):
        summary = wingman_sge_stat.get_host_summary()
        summary['volumes'] = self.volume_history.get_volume_stats()
        return summary

    # TODO: Switch this to look up runs by name, not run_dir
    def get_run_tasks(self, run_dir):
        with self.transaction() as db:
            result = []
            log.warn("getting run_id")
            run_id = self._assert_run_valid(run_dir)
            db.execute("SELECT task_dir, status, try_count, node_name, external_id, group_number FROM TASKS WHERE run_id = ?", [run_id])
            for task_dir, status, try_count, node_name, external_id, group_number in db.fetchall():
                task = {'task_dir':task_dir, 'status':status_code_to_name[status], 'try_count': try_count, 'node_name':node_name, 'external_id':external_id, 'group_number':group_number}
                result.append(task)
            return result

    def get_run(self, name):
        with self.transaction() as db:
            db.execute("SELECT run_dir, name, parameters, added_tags FROM RUNS WHERE name = ?", [name])
            rows = db.fetchall()
            assert len(rows) == 1
            run_dir, name, parameters, added_tags = rows[0]
            if added_tags == "" or added_tags == None:
                added_tags = {}
            else:
                added_tags = json.loads(added_tags)
            return dict(run_dir=run_dir, name=name, parameters=parameters, added_tags=added_tags)

    def set_tag(self, name, property, value):
        with self.transaction() as db:
            db.execute("SELECT added_tags FROM RUNS WHERE name = ?", [name])
            rows = db.fetchall()
            assert len(rows) == 1
            added_tags_json = rows[0][0]

            if added_tags_json == "" or added_tags_json == None:
                added_tags = {}
            else:
                added_tags = json.loads(added_tags_json)

            added_tags[property] = value
            added_tags_json = json.dumps(added_tags)

            db.execute("UPDATE RUNS SET added_tags = ? WHERE name = ?", [added_tags_json, name])
            return True

    def get_runs(self, archive_name=None):
        with self.transaction() as db:
            params = []
            query = "SELECT run_id, run_dir, name, parameters, added_tags FROM RUNS"
            if archive_name != None:
                query += " WHERE archive_name = ?"
                params.append(archive_name)
            else:
                query += " WHERE archive_name is null"
            db.execute(query, params)
            rows = db.fetchall()
            result = []
            for run_id, run_dir, name, parameters, added_tags in rows:
                if parameters != None:
                    parameters = json.loads(parameters)
                if added_tags != None:
                    added_tags = json.loads(added_tags)

                db.execute("SELECT status, count(1) FROM TASKS WHERE run_id = ? GROUP BY status", [run_id])
                summary = {}
                for status, count in db.fetchall():
                    summary[status_code_to_name[status]] = count
                result.append(dict(run_id=run_id, run_dir=run_dir, name=name, parameters=parameters, added_tags=added_tags, status=summary))
            return result

    def get_version(self):
        return "1"

    def run_created(self, run_dir, name, config_path, parameters):
        with self.transaction() as db:
            db.execute("INSERT INTO RUNS (run_dir, name, flock_config_path, parameters) VALUES (?, ?, ?, ?)", [run_dir, name, config_path, parameters])
            run_id = db.lastrowid
            return run_id

    def _assert_path_sane(self, path):
        components = path.split("/")
        for c in components:
            assert c != ".."
            assert c != "" # make sure no leading slash

    def _assert_filename_sane(self, filename):
        assert not ("/" in filename)

    def _assert_run_valid(self, run_dir):
        with self.transaction() as db:
            db.execute("SELECT count(1), min(run_id) FROM RUNS WHERE run_dir = ?", [run_dir])
            counts = db.fetchall()
            assert len(counts) == 1
            assert counts[0][0] == 1, "could not find run with run_dir %s" % (run_dir)
            return counts[0][1]

    def _get_run_dir_for_run_name(self, run_name):
        with self.transaction() as db:
            db.execute("SELECT count(1), min(run_id), min(run_dir) FROM RUNS WHERE name = ?", [run_name])
            counts = db.fetchall()
            assert len(counts) == 1
            assert counts[0][0] == 1, "could not find run with name %s" % (run_name)
            return counts[0][2]

    def get_run_files(self, run_name, wildcard):
        run_dir = self._get_run_dir_for_run_name(run_name)
        self._assert_path_sane(wildcard)
        filenames = glob.glob(os.path.join(run_dir, wildcard))

        result = []
        for filename in filenames:
            name = filename[len(run_dir)+1:]
            s = os.stat(filename)
            is_dir = os.path.isdir(filename)
            result.append(dict(name=name, size=s.st_size, mtime=s.st_mtime, is_dir=is_dir))

        return result

    def get_file_content(self, run_name, path, offset, length):
        run_dir = self._get_run_dir_for_run_name(run_name)
        self._assert_path_sane(path)
        assert length < 1000000
        fd = open(os.path.join(run_dir, path))
        fd.seek(offset)
        buffer = fd.read(length)
        fd.close()

        return dict(data=base64.standard_b64encode(buffer), md5=hashlib.md5(buffer).hexdigest())

    def run_submitted(self, run_dir, name, config_path, parameters):
        run_id = self.run_created(run_dir, name, config_path, parameters)

        notify_command = format_notify_command(self.flock_home, self.endpoint_url)
        config = flock_config.load_config([config_path], run_dir, {})
        task_definition_path = flock.write_files_for_running(self.flock_home, notify_command, run_dir, config.invoke, None, config.environment_variables, config.language)
        return dict(run_id = run_id, task_dirs = self.taskset_created(run_dir, task_definition_path))

    def taskset_created(self, run_dir, task_definition_path):
        full_task_dir_paths = []
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
                full_task_dir_path = os.path.join(run_dir, task_dir)
                db.execute("INSERT INTO TASKS (run_id, task_dir, status, try_count, group_number, external_id) values (?, ?, ?, 0, ?, ?)", [run_id, full_task_dir_path, status, group, external_id])
                full_task_dir_paths.append(full_task_dir_path)

            self._cv_created.notify_all()
        return full_task_dir_paths

    def wait_for_created(self, timeout):
        self._lock.acquire()
        self._cv_created.wait(timeout)
        self._lock.release()

    def list_archives(self):
        with self.transaction() as db:
            db.execute("SELECT distinct archive_name FROM RUNS")
            rows = db.fetchall()
            archive_names = [x[0] for x in rows]

            return archive_names

    def archive_run(self, name, archive_name):
        self._assert_filename_sane(archive_name)
        with self.transaction() as db:
            db.execute("SELECT run_id, run_dir, name FROM RUNS WHERE name = ?", [name])
            rows = db.fetchall()
            if len(rows) != 1:
                return None

            run_id, run_dir, run_name = rows[0]

            # a bit of a hack: We actually store the runs in a path like .../20150101-100101/files.  However, it's preferable to get the parent which contains the config.json and
            # maybe other small files
            run_dir = os.path.dirname(run_dir)
            if os.path.exists(run_dir):
                new_run_dir = os.path.join(self._archive_path, archive_name, os.path.basename(run_dir))
            else:
                if archive_name == "trash":
                    new_run_dir = None
                else:
                    raise Exception("Run directory %r does not exist.  Archive to 'trash' to remove run." % (run_dir))

            db.execute("DELETE FROM TASKS WHERE run_id = ?", [run_id])

            if new_run_dir != None:
                log.warn("renaming %s to %s, name=%s" % (run_dir, new_run_dir, run_name))
                db.execute("UPDATE RUNS SET archive_name = ?, run_dir = ? WHERE run_id = ?", [archive_name, new_run_dir, run_id])
                os.renames(run_dir, new_run_dir)
                return new_run_dir
            else:
                log.warn("deleting run %s from db (%s does not exist)", run_id, run_dir)
                db.execute("DELETE FROM RUNS WHERE run_id = ?", [run_id])
                return ""


    def delete_run(self, run_dir):
        with self.transaction() as db:
            db.execute("SELECT run_id FROM RUNS WHERE run_dir = ?", [run_dir])
            rows = db.fetchall()
            if len(rows) == 1:
                run_id = rows[0][0]

                db.execute("DELETE FROM TASKS WHERE run_id = ?", [run_id])
                db.execute("DELETE FROM RUNS WHERE run_id = ?", [run_id])
        return True

    def task_submitted(self, task_dir, external_id):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET update_time = ?, status = ?, external_id = ? WHERE task_dir = ?", [time.time(), SUBMITTED, external_id, task_dir])
            if db.rowcount == 0:
                log.warn("task_submitted(%s, %s) called, but no record in db", task_dir, external_id)
        return True

    def task_started(self, task_dir, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET update_time = ?, try_count = try_count + 1, node_name = ?, status = ? WHERE task_dir = ?", [time.time(), node_name, STARTED, task_dir])
            if db.rowcount == 0:
                log.warn("task_started(%s, %s) called, but no record in db", task_dir, node_name)
        return True


    def set_task_status(self, task_dir, status):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE task_dir = ?", [time.time(), status, task_dir])
            if db.rowcount == 0:
                log.warn("set_task_status(%s, %s) called, but no record in db", task_dir, status)
        return True

    def task_failed(self, task_dir):
        return self.set_task_status(task_dir, FAILED)

    def task_missing(self, task_dir):
        return self.set_task_status(task_dir, MISSING)

    def task_completed(self, task_dir):
        return self.set_task_status(task_dir, COMPLETED)

    def mark_stale_missing_tasks_as_failed(self, max_stale_secs=15*60):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE update_time < ? and STATUS = ?", [time.time(), FAILED, time.time() - max_stale_secs, MISSING])


    def node_disappeared(self, node_name):
        with self.transaction() as db:
            db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE node_name = ? and status in (?, ?)", [time.time(), WAITING, node_name, STARTED, MISSING])
        return True

    def retry_run(self, run_dir):
        with self.transaction() as db:
            db.execute("SELECT run_id FROM RUNS WHERE run_dir = ?", [run_dir])
            rows = db.fetchall()
            if len(rows) == 1:
                run_id = rows[0][0]
                # treating MISSING as the same as FAILED.  Perhaps there should be something that transforms MISSING tasks to FAILED after some timeout
                db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE run_id = ? and status in (?, ?, ?, ?, ?)", [time.time(), WAITING, run_id, FAILED, KILLED, MISSING, PREREQ_FAILED, QUOTA_EXCEEDED])
        return True

    def kill_run(self, run_dir):
        with self.transaction() as db:
            db.execute("SELECT run_id FROM RUNS WHERE run_dir = ?", [run_dir])
            rows = db.fetchall()
            if len(rows) == 1:
                run_id = rows[0][0]

                db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE run_id = ? and status in (?, ?)", [time.time(), KILL_PENDING, run_id, SUBMITTED, STARTED])
                db.execute("UPDATE TASKS SET update_time = ?, status = ? WHERE run_id = ? and status in (?, ?)", [time.time(), KILLED, run_id, READY, WAITING])

                # wake up main loop which is going to perform the actual killing
                self._cv_created.notify_all()

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

    def find_tasks_external_id_by_status(self, status, limit=None):
        if limit == 0:
            return []

        query = "SELECT external_id, task_dir FROM tasks WHERE status = ?"
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

    def count_tasks_by_group_number(self, run_id):
        # record of the form (finished_count, failed_count, running_count, waiting_count)
        result = collections.defaultdict(lambda: [0,0,0,0])
        as_map = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        with self.transaction() as db:
            db.execute("SELECT group_number, status, count(*) FROM tasks WHERE run_id = ? group by group_number, status",
                       [run_id])
            for number, status, count in db.fetchall():
                record = result[number]
                as_map[number][status_code_to_name[status]] += count
                if status in [COMPLETED]:
                    record[0] += 1
                elif status in [KILLED, FAILED, PREREQ_FAILED, QUOTA_EXCEEDED]:
                    record[1] += 1
                elif status in [WAITING]:
                    record[3] += 1
                else:
                    record[2] += 1

        log.warn("count_tasks_by_group_number(%r): as_map=%r, result=%r", run_id, dict([(k, dict(v)) for k,v in as_map.items()]), dict(result))
        return result

    def get_config_path(self, run_id):
        with self.transaction() as db:
            db.execute("SELECT run_dir, flock_config_path FROM RUNS WHERE run_id = ?", [run_id])
            return db.fetchall()[0]

    def get_required_mem_override(self, run_id):
        with self.transaction() as db:
            db.execute("SELECT required_mem_override FROM RUNS WHERE run_id = ?", [run_id])
            return db.fetchall()[0][0]

    def set_required_mem_override(self, run_id, mem_override):
        with self.transaction() as db:
            db.execute("UPDATE RUNS set required_mem_override = ? WHERE run_dir = ?", [mem_override, run_id])
        return True

def handle_kill_pending_tasks(store, queue, batch_size=100):
    external_id_and_task_dirs = dict(store.find_tasks_external_id_by_status(KILL_PENDING, limit=batch_size))
    log.info("handle_kill_pending_tasks: %s", repr(external_id_and_task_dirs))
    if len(external_id_and_task_dirs) > 0:
        # strip off the queue prefix
        external_ids = [external_id.split(":")[1] for external_id in external_id_and_task_dirs.keys()]

        log.info("Killing tasks with external_ids: %s", repr(external_ids))
        tasks = [flock.Task(None, external_id, None, None) for external_id in external_ids]
        queue.kill(tasks)
        # just let the jobs transition to MISSING in next periodic check.  Should we explictly mark these as killed?
        # seems like many ways for that to fall out of sync with the backend queue if we set it to killed without checking

    for external_id, task_dir in external_id_and_task_dirs.items():
        store.set_task_status(task_dir, KILL_SUBMITTED)

    return False

def update_tasks_which_disappeared(store, external_ids_of_actually_in_queue, external_id_to_task_dir, state_to_use_if_missing):
    external_ids_of_those_we_think_are_submitted = set(external_id_to_task_dir.keys())

    # identify tasks which transitioned from running -> not running
    # and call these "missing" (assuming the db still claims these are running).  All other transitions
    # should already be performed through other means.

    disappeared_external_ids = external_ids_of_those_we_think_are_submitted - external_ids_of_actually_in_queue
    log.info("external_ids_of_actually_in_queue = %s", external_ids_of_actually_in_queue)
    log.info("disappeared_external_ids = %s", disappeared_external_ids)
    for external_id in disappeared_external_ids:
        task_dir = external_id_to_task_dir[external_id]

        # check the filesystem to see if it really did succeed and we just missed the notification
        if flock.finished_successfully(None, task_dir):
            store.task_completed(task_dir)
        else:
            store.set_task_status(task_dir, state_to_use_if_missing)

def identify_tasks_which_disappeared(store, queue):
    log.info("calling identify_tasks_which_disappeared")
    external_ids_of_actually_in_queue = set([(queue.external_id_prefix + x) for x in queue.get_jobs_from_external_queue().keys()])

    # handle all the submitted jobs
    external_id_to_task_dir = dict([(external_id, task_dir) for task_dir, external_id in store.find_external_ids_of_submitted()])
    update_tasks_which_disappeared(store, external_ids_of_actually_in_queue, external_id_to_task_dir, MISSING)

    # handle all of the killed jobs
    external_id_to_task_dir = dict(store.find_tasks_external_id_by_status(KILL_SUBMITTED))
    update_tasks_which_disappeared(store, external_ids_of_actually_in_queue, external_id_to_task_dir, KILLED)

def update_waiting_tasks(store):
    waiting_tasks = store.find_tasks_by_status(WAITING)
    log.info("Found %d WAITING tasks", len(waiting_tasks))
    for run_id, task_dir, group in waiting_tasks:
        counts_by_group = store.count_tasks_by_group_number(run_id)

        # check to make sure that we've completed everything in groups earlier then this one
        prereq_finished = True
        prereq_failed = False
        for other_group, counts in counts_by_group.items():
            if other_group >= group:
                continue
            finished_count, failed_count, running_count, waiting_count = counts
            if failed_count > 0:
                prereq_failed = True
            elif running_count > 0 or waiting_count > 0:
                prereq_finished = False

        if prereq_failed:
            store.set_task_status(task_dir, PREREQ_FAILED)
        elif prereq_finished:
            log.info("Transitioning %s to READY. [counts_by_group=%s]", task_dir, counts_by_group)
            store.set_task_status(task_dir, READY)
        else:
            log.debug("Could not run %s because needs to wait for another job", task_dir)

def submit_created_tasks(listener, store, queue_factory, max_submitted):
    submitted_count = len(store.find_tasks_by_status(SUBMITTED))

    # process all the waiting to make sure they've met their requirements
    update_waiting_tasks(store)

    # submit any ready tasks
    submit_count = max(0, max_submitted-submitted_count)
    tasks = store.find_tasks_by_status(READY, limit=submit_count)
    log.info("Found %d READY tasks", len(tasks))
    queue_cache = {}
    for run_id, task_dir, group in tasks:
        try:
            if not (run_id in queue_cache):
                log.info("Creating queue missing from cache for %s", run_id)
                run_dir, config_path = store.get_config_path(run_id)
                required_mem_override = store.get_required_mem_override(run_id)

                config = flock_config.load_config([config_path], run_dir, {})
                queue = queue_factory(listener, config.qsub_options, config.scatter_qsub_options, config.gather_qsub_options, config.name, config.workdir, required_mem_override)
                queue_cache[run_id] = queue

            queue = queue_cache[run_id]

            queue.submit(run_id, os.path.join(run_dir, task_dir), flock.guess_task_type(task_dir))
        except:
            log.exception("Got exception submitting %s %s", run_dir, task_dir)
            store.set_task_status(task_dir, FAILED)


def main_loop(endpoint_url, flock_home, store, max_submitted, localQueue = False):

    if localQueue:
        queue_factory = lambda listener, qsub_options, scatter_qsub_options, gather_qsub_options, name, workdir, required_mem_override: LocalBgQueue(listener, workdir)
    else:
        queue_factory = lambda listener, qsub_options, scatter_qsub_options, gather_qsub_options, name, workdir, required_mem_override: SGEQueue(listener, qsub_options, scatter_qsub_options, gather_qsub_options, name, workdir, required_mem_override)

    listener = wingman_client.ConsolidatedMonitor(endpoint_url, flock_home)
    t_queue = queue_factory(None, None, None, None, "", "./", None)

    last_check_for_missing = None

    # periodic tasks
    while True:
        try:
            store.update_volume_history()
            
            store.mark_stale_missing_tasks_as_failed()

            needed_to_kill_tasks = handle_kill_pending_tasks(store, t_queue)
            submit_created_tasks(listener, store, queue_factory, max_submitted)

            if last_check_for_missing == None or (time.time() - last_check_for_missing) > 60:
                identify_tasks_which_disappeared(store, t_queue)
                last_check_for_missing = time.time()

            # only sleep if we didn't have to kill any tasks.  If we did have to kill tasks, then
            # don't sleep and immediately poll again in case there are more tasks to kill.
            if not needed_to_kill_tasks:
                store.wait_for_created(10)
        except:
            traceback.print_exc()

def make_function_wrapper(fn):
    def wrapped(*args, **kwargs):
        print "%s(%s, %s)" % (fn.__name__, args, kwargs)
        try:
            return fn(*args, **kwargs)
        except:
            traceback.print_exc()
            raise
    return wrapped

import argparse

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

def main():
    trace_on_demand.install()
    FORMAT = "[%(asctime)-15s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO, datefmt="%Y%m%d-%H%M%S")

    parser = argparse.ArgumentParser(description='Wingman service for tracking state of jobs.')
    parser.add_argument('queue', help='The queue to use.  Either "local" or "sge"')
    parser.add_argument('db_path', help="The path to the sqlite3 database to use for bookkeeping.  It will be created if it doesn't already exist")
    parser.add_argument("port", help="The port this service should listen on", type=int)
    parser.add_argument("--maxsubmitted", help="The maximum number non-running jobs allowed to sit in the backend queue at one time", type=int, default=100)

    args = parser.parse_args()

    queue = args.queue
    db = args.db_path
    port = args.port
    
    flock_home = flock.get_flock_home()
    endpoint_url = "http://%s:%d" % (socket.gethostname(), port)

    store = TaskStore(db, flock_home, endpoint_url=endpoint_url)

    assert queue in ['local', 'sge']

    main_loop_thread = threading.Thread(target=lambda: main_loop(endpoint_url, flock_home, store, args.maxsubmitted, localQueue=(queue == 'local')))
    main_loop_thread.daemon = True
    server = ThreadedXMLRPCServer(("0.0.0.0", port), allow_none=True)
    main_loop_thread.start()

    print "Listening on port %d..." % port
    for method in ["get_run_files", "get_file_content", "delete_run", "retry_run", "kill_run", "run_created", "run_submitted", "taskset_created", "task_submitted", "task_started",
                   "task_failed", "task_completed", "node_disappeared", "get_version", "get_runs", "set_required_mem_override",
                   "get_run_tasks", "get_run", "set_tag", "archive_run", "list_archives", "get_host_summary"]:
        server.register_function(make_function_wrapper(getattr(store, method)), method)

    server.serve_forever()

if __name__ == "__main__":
    logging.basicConfig(format=flock.FORMAT, level=logging.INFO, datefmt="%Y%m%d-%H%M%S")
    main()
