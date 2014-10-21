import glob
import os
import sys
import xmlrpclib

def import_run(service, run_dir):
    print "importing %s" % run_dir
    parameters = open(os.path.dirname(run_dir)+"/config.json").read()
    service.run_created(run_dir, os.path.basename(os.path.dirname(run_dir)), os.path.dirname(run_dir)+"/config", parameters)

    for dirname in glob.glob("%s/tasks*" % run_dir):
        fn = "%s/task_dirs.txt" % dirname
        if os.path.exists(fn):
            service.taskset_created(run_dir, fn)

if __name__ == "__main__":
    url_endpoint = sys.argv[1]
    service = xmlrpclib.ServerProxy(url_endpoint)

    for dirname in sys.argv[2:]:
        import_run(service, dirname+"/files")
