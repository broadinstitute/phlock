import sys
from fabric.api import run, local, put, get, cd, settings
import fabric.contrib.files
import fabric.network
import tempfile
import datetime
import logging

FLOCK_PATH = "/xchip/flock/bin/flockjob"
CODE_DIR = "/xchip/datasci/code-cache"
logging.basicConfig(level=logging.WARN)
log = logging.getLogger("syncRuns")

def exists(path, verbose=False):
    return run("/usr/bin/test -e %s" % path, warn_only=True, quiet=True).return_code == 0

def find_remote_runs(remote_dir):
    files_str = run("ls %s/*-*" % remote_dir)
    print files_str.split("\n")
    assert False

def sync():
    local_dir = "/xchip/datasci/ec2-runs"
    remote_dir = "/data2/runs"

    for run_id in find_remote_runs(remote_dir):
        remote_run_dir = os.path.join(remote_dir, run_id)
        remote_tar = os.path.join(remote_dir, run_id+".tar.gz")
        local_tar = os.path.join(local_dir, run_id+".tar.gz")
    
        if os.path.exists(os.path.join(root, run_id)):
            print "already exists %s" % run_id
        else:
            run("python /xchip/scripts/bundle_run_dirs.py %s > %s" % ())
            get(remote_tar, local_tar)
            run("rm %s" % remote_tar)
            local("cd %s ; tar -xzf %s" % (local_dir, local_tar) )
            local("rm %s" % local_tar)
            run("rm -r %s" % remote_run_dir)

def deploy(host, key_filename):
    try:
        with settings(host_string=host, key_filename=key_filename, user="ubuntu"):
            sync()
    finally:
        fabric.network.disconnect_all()

if __name__ == "__main__":
    deploy(*sys.argv[1:])

