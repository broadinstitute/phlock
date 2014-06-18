from fabric.api import run, local, put, get, cd, settings
import fabric.network

timestamp = "20140601"
repo = "ssh://git@stash.broadinstitute.org:7999/cpds/atlantis.git"
branch = "HEAD"
target_dir = "/xchip/datasci/runs/"+timestamp
zip_temp_file = "/tmp/zip"
config_temp_file = "/tmp/config"

def deploy():
  run("mkdir -p "+target_dir)
  local("git archive --remote "+repo+" -o "+zip_temp_file+" --format=zip "+ branch)
  put(config_temp_file, target_dir+"/config")
  put(zip_temp_file, target_dir+"/code.zip")
  with cd(target_dir):
    run("mkdir code")
  with cd(target_dir+"/code"):
    run("unzip ../code.zip")
    run("flock --rundir "+target_dir+"/files ../config")

try:
  with settings(host_string="datasci-dev", key_filename=None):
    deploy()
finally:
  fabric.network.disconnect_all()

