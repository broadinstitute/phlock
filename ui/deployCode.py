import sys
from fabric.api import run, local, put, get, cd, settings
import fabric.contrib.files
import fabric.network
import tempfile
import logging

CODE_DIR = "/data2/code-cache"
logging.basicConfig(level=logging.WARN)
log = logging.getLogger("remoteExec")

def exists(path, verbose=False):
    return run("/usr/bin/test -e %s" % path, warn_only=True, quiet=True).return_code == 0

def deploy_code_from_git(repo, sha, branch):
    sha_code_dir = CODE_DIR+"/"+sha
    if not exists(sha_code_dir, verbose=True):
        log.info("Deploying code to %s" % sha_code_dir)

        # create target directory where the code will live
        run("mkdir -p "+sha_code_dir)

        # construct zip file from git and copy to host
        with tempfile.NamedTemporaryFile() as zip_temp_file:
            zip_temp_file_name = zip_temp_file.name
            # it makes me sad that I can't archive by sha, so there's a race condition here.  The solution appears to be
            # to locally mirror the repo, but this such a small volume project, I'll delay implementing that.
            local("git archive --remote "+repo+" -o "+zip_temp_file_name+" --format=zip "+branch)
    
            target_zip_file = CODE_DIR+"/"+sha+".zip"
            put(zip_temp_file_name, target_zip_file)

        # extract the file into the code directory and clean up            
        with cd(sha_code_dir):
            run("unzip "+target_zip_file)
        run("rm "+target_zip_file)
    else:
        log.warn("Code already deployed to %s, skipping deploy" % sha_code_dir)

    return sha_code_dir

import paramiko


def deploy(host, key_filename, repo, branch, sha):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username="ubuntu", key_filename=key_filename)

    try:
        with settings(host_string=host, key_filename=key_filename, user="root"):
            deploy_code_from_git(repo, sha, branch)

    finally:
        fabric.network.disconnect_all()

if __name__ == "__main__":
    deploy(*sys.argv[1:])



