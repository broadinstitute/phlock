from nose import with_setup
import tempfile
import os
import os.path
import shutil
import flock.queue

task_dir = None

def setup_task_dir():
    global task_dir
    task_dir = tempfile.mkdtemp()

    for fn in ["stdout.txt", "stderr.txt", "stdout-1.txt", "stderr-1.txt"]:
      with open(task_dir+"/"+fn, "w") as fd:
          fd.write("txt")

def cleanup_task_dir():
    global task_dir
    if os.path.exists(task_dir):
        shutil.rmtree(task_dir)
    task_dir = None

@with_setup(setup_task_dir, cleanup_task_dir)
def test_cleanup_task():
  flock.queue.clean_task_dir(task_dir)
  assert not os.path.exists(task_dir+"/stdout.txt")
  assert not os.path.exists(task_dir+"/stderr.txt")
  assert os.path.exists(task_dir+"/stdout-2.txt")
  assert os.path.exists(task_dir+"/stderr-2.txt")
  
