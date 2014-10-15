__author__ = 'pmontgom'

import sys
import xmlrpclib
import socket

def main(args):
    if len(args) == 0:
        print "Usage: ENDPOINT started|completed|failed run_id task_dir"
        sys.exit(-1)

    service = xmlrpclib.ServerProxy(args[0])
    run_id = args[2]
    task_dir = args[3]

    if args[1] == "started":
        node_name = socket.gethostname()
        service.task_started(run_id, task_dir, node_name)
    elif args[1] == "failed":
        service.task_failed(run_id, task_dir)
    elif args[1] == "failed":
        service.task_completed(run_id, task_dir)
    else:
        raise Exception("expected either started, completed or failed")

if __name__ == "__main__":
    main(sys.argv[1:])