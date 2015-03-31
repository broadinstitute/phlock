__author__ = 'pmontgom'

import __init__ as flock
import logging
import argparse
import wingman

log = logging.getLogger("flock")

def main():
    FORMAT = "[%(asctime)-15s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO, datefmt="%Y%m%d-%H%M%S")

    parser = argparse.ArgumentParser(description='Updates finished jobs by looking at the filesystem')
    parser.add_argument('db_path', help="The path to the sqlite3 database to use for bookkeeping.  It will be created if it doesn't already exist")

    args = parser.parse_args()

    db = args.db_path

    store = wingman.TaskStore(db, flock_home=None, endpoint_url=None)
    with store.transaction() as db:
        db.execute("SELECT task_dir FROM TASKS WHERE status = ?", [wingman.COMPLETED])
        for task_dir, status in db.fetchall():
            if not flock.finished_successfully(None, task_dir):
                log.info("missing %s", task_dir)
                store.set_task_status(task_dir, wingman.MISSING)

if __name__ == "__main__":
    main()