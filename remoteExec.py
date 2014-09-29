import sys
from fabric.api import run, local, put, get, cd, settings
import fabric.contrib.files
import fabric.network
import tempfile
import datetime
import logging

CODE_DIR = "/xchip/datasci/code-cache"
logging.basicConfig(level=logging.WARN)
log = logging.getLogger("remoteExec")

def get_sha(repo, branch):
    stdout = local("git ls-remote %s"%(repo,), capture=True)
    pairs = [line.split("\t") for line in stdout.split("\n")]
    for sha, tag in pairs:
        if tag == branch:
            return sha
    raise Exception("Could not find %s in %s" % (branch, repr(pairs)))

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

def install_config(target_root, sha_code_dir, config_temp_file, timestamp):
    target_dir = target_root+"/"+timestamp

    # create the directory for this run
    run("mkdir -p "+target_dir)

    # upload the config file and run via flock, after setting the working directory to be the current code dir
    put(config_temp_file, target_dir+"/config")

    return sha_code_dir, target_dir, "bash "+target_dir+"/flock-wrapper.sh run"

def install_wrapper_script(sha_code_dir, target_dir, flock_path):
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file_name = temp_file.name
        temp_file.write("#!/bin/bash\n")
        temp_file.write("cd %s\n" % sha_code_dir)
        temp_file.write("echo retrying... >> "+target_dir+"/output.txt\n")
        temp_file.write(flock_path+" --rundir "+target_dir+"/files \"$1\" "+target_dir+"/config 2>&1 | tee -a "+target_dir+"/output.txt\n")
        temp_file.flush()

        target_script = target_dir+"/flock-wrapper.sh"
        put(temp_file_name, target_script)

class EchoAndCapture(object):
    def __init__(self, filename):
        self.f = open(filename, "w")

    def write(self, buffer):
        sys.stdout.write(buffer)
        return self.f.write(buffer)

    def flush(self):
        self.f.flush()

    def close(self):
        self.f.close()

import json

def deploy(host, key_filename, repo, branch, config_file, target_root, json_params, timestamp, flock_path):
    try:
        with settings(host_string=host, key_filename=key_filename, user="root"):
            sha = get_sha(repo, branch)
            sha_code_dir = deploy_code_from_git(repo, sha, branch)

            params = json.loads(open(json_params).read())
            params["sha"] = sha
            with open(json_params, "w") as fd:
                fd.write(json.dumps(params))

        with settings(host_string=host, key_filename=key_filename, user="ubuntu"):
            working_dir, target_dir, command = install_config(target_root, sha_code_dir, config_file, timestamp)
            put(json_params, target_dir+"/config.json")
            with cd(working_dir):
                install_wrapper_script(working_dir, target_dir, flock_path)
                #stdout_capture = EchoAndCapture(target_dir+"/output.txt")
                run(command)
                #stdout_capture.close()
    finally:
        fabric.network.disconnect_all()

if __name__ == "__main__":
    deploy(*sys.argv[1:])

