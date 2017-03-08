import sqlite3
import os
import sys

COMPLETED = 5
FAILED = -1

filename = sys.argv[1]
dir_prefix = sys.argv[2]

failures = set()
conn = sqlite3.connect(filename)
c = conn.cursor()
c.execute("select task_dir from tasks where task_dir like ? and status = ?", (dir_prefix+"%", COMPLETED))
for task_dir, in c.fetchall():
    outputs = os.listdir(task_dir)
    for output in outputs:
        fn = os.path.join(task_dir, output)
        if os.path.getsize(fn) == 0:
            failures.add(task_dir)
    print("Checked {}".format(task_dir))

print("failures: {}".format(",".join(failures)))
print("# of failures: {}".format(len(failures)))
for failure in failures:
    c.execute("update tasks set status = ? where task_dir = ?", (FAILED, failure))
conn.commit()
