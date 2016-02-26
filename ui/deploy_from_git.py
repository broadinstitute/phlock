import sys
from fabric.api import run, local, put, get, cd, settings
import fabric.contrib.files
import fabric.network
import tempfile
import logging
import json
import sshxmlrpc
import uuid
import os.path

logging.basicConfig(level=logging.WARN)
log = logging.getLogger("remoteExec")

def exists(path, verbose=False):
    return run("/usr/bin/test -e %s" % path, warn_only=True, quiet=True).return_code == 0

def get_random_id():
    return str(uuid.uuid4())

def deploy_code_from_git(sha_code_dir, repo, branch):
    code_dir = os.path.dirname(sha_code_dir)
    random_fn = get_random_id()
    temp_code_dir = code_dir+"/.temp-"+random_fn
    if not exists(sha_code_dir, verbose=True):
        print("Deploying code to %s" % sha_code_dir)

        # construct zip file from git and copy to host
        with tempfile.NamedTemporaryFile() as zip_temp_file:
            zip_temp_file_name = zip_temp_file.name
            # it makes me sad that I can't archive by sha, so there's a race condition here.  The solution appears to be
            # to locally mirror the repo, but this such a small volume project, I'll delay implementing that.
            local("git archive --remote "+repo+" -o "+zip_temp_file_name+" --format=zip "+branch)
    
            target_zip_file = code_dir+"/.temp-"+random_fn+".zip"
            put(zip_temp_file_name, target_zip_file)

        # create target directory where the code will live
        run("mkdir -p "+temp_code_dir)

        # extract the file into the code directory and clean up            
        with cd(temp_code_dir):
            run("unzip "+target_zip_file)

        run("if [ -r {temp_code_dir}/setup_for_clusterui_run.sh ]; then cd {temp_code_dir} && bash {temp_code_dir}/setup_for_clusterui_run.sh ; fi".format(temp_code_dir=temp_code_dir))
        run("mv "+temp_code_dir+" "+sha_code_dir)
        run("rm "+target_zip_file)
    else:
        print("Code already deployed to %s, skipping deploy" % sha_code_dir)

def deploy(host, key_filename, repo, branch, sha_code_dir):
    with settings(host_string=host, key_filename=key_filename, user="root"):
        deploy_code_from_git(sha_code_dir, repo, branch)

if __name__ == "__main__":
    try:
        deploy(*sys.argv[1:])
    finally:
        fabric.network.disconnect_all()


