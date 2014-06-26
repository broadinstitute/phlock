import sys
from fabric.api import run, local, put, get, cd, settings
import fabric.contrib.files
import fabric.network
import tempfile
import datetime

FLOCK_PATH = "/xchip/flock/bin/flockjob"
CODE_DIR = "/xchip/datasci/code-cache"

def get_sha(repo, branch):
    stdout = local("git ls-remote %s"%(repo,), capture=True)
    pairs = [line.split("\t") for line in stdout.split("\n")]
    for sha, tag in pairs:
        if tag == branch:
            return sha
    raise Exception("Could not find %s in %s" % (branch, repr(pairs)))

def exists(path, verbose=False):
    return run("/usr/bin/test -e %s" % path, warn_only=True).return_code == 0

def deploy_code_from_git(repo, branch):
    sha = get_sha(repo, branch)
    sha_code_dir = CODE_DIR+"/"+sha
    if not exists(sha_code_dir, verbose=True):
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

    return sha_code_dir


def install_config(sha_code_dir, config_temp_file):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    target_dir = "/xchip/datasci/runs/"+timestamp

    # create the directory for this run
    run("mkdir -p "+target_dir)

    # upload the config file and run via flock, after setting the working directory to be the current code dir
    put(config_temp_file, target_dir+"/config")

    return sha_code_dir, FLOCK_PATH+" --rundir "+target_dir+"/files run "+target_dir+"/config"


def deploy(host, key_filename, repo, branch, config_file):
    try:
        with settings(host_string=host, key_filename=key_filename, user="root"):
            sha_code_dir = deploy_code_from_git(repo, branch)
        with settings(host_string=host, key_filename=key_filename, user="ubuntu"):
            working_dir, command = install_config(sha_code_dir, config_file)
            with cd(working_dir):
                run(command)
    finally:
        fabric.network.disconnect_all()

if __name__ == "__main__":
    deploy(*sys.argv[1:])

