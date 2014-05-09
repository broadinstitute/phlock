import argparse
import sys
import collections
import os
import csv
import glob
import subprocess

# the various status codes for tasks
CREATED = "Created"
SUBMITTED = "Submitted"
FINISHED = "Finished"

Task = collections.namedtuple("Task", ["task_dir", "external_id", "status", "full_path"])

def system(cmd):
  print "EXEC %s:" % repr(cmd)
  retcode = subprocess.call(cmd, env=modified_env, shell=True)
  if retcode != 0:
    raise Exception("Command terminated with exit status = %d" % retcode)
  #os.system(cmd)

def read_task_dirs(run_id):
  grouped_commands = {}
  for dirname in glob.glob("%s/job*" % run_id):
    fn = "%s/task_dirs.txt" % dirname
    if os.path.exists(fn):
      with open(fn) as fd:
        for line in fd.readlines():
          line = line.strip()
          i=line.find(" ")
          group = int(line[:i])
          command = line[i+1:]
          
          if group in grouped_commands:
            commands = grouped_commands[group]
          else:
            commands = []
            grouped_commands[group] = commands
          
          commands.append(command)
  keys = grouped_commands.keys()
  keys.sort()
  
  return [grouped_commands[k] for k in keys]
  
def is_finished(run_id, task_dir):
  finished = os.path.exists("%s/%s/finished-time.txt" % (run_id, task_dir))
  #print "is_finished %s/finished-time.txt -> %s" % (task_dir, finished)
  return finished

class LocalQueue(object):
  def __init__(self):
    self._ran = set()
  
  def find_tasks(self, run_id):
    grouped_dirs = read_task_dirs(run_id)
    tasks = []
    for task_dirs in grouped_dirs:
      tasks.extend([ Task(task_dir, None, FINISHED if is_finished(run_id, task_dir) else CREATED, run_id+"/"+task_dir) for task_dir in task_dirs ])
    return tasks
    
  def submit(self, task):
    d = task.full_path
    cmd = "bash %s/task.sh > %s/stdout.txt 2> %s/stderr.txt" % (d,d,d)
    if cmd in self._ran:
      raise Exception("Already ran %s once" % cmd)
    system(cmd)
    self._ran.add(cmd)
    
  def kill(self, task):
    raise Exception("not implemented")

class LsfQueue(object):
  def get_active_lsf_jobs(self):
    # exec bjobs 
    raise Exception("unimplemented")
    
  def read_task_external_ids(self, run_id):
    raise Exception("unimplemented")
  
  def find_tasks(self, run_id):
    task_dirs = read_task_dirs(run_id)
    external_ids = self.read_task_external_ids(run_id)
    active_lsf_jobs = self.get_active_lsf_jobs()
    
    def get_status(task_dir):
      if task_dir in external_ids:
        lsf_id = external_ids[task_dir]
        if lsf_id in active_lsf_jobs:
          return SUBMITTED
        else:
          return FINISHED
      else:
        return CREATED
    
    def get_external_id(task_dir):
      if task_dir in external_ids:
        return external_ids[task_dir]
      else:
        return None
        
    return [ Task(task_dir, get_external_id(task_dir), get_status(task_dir)) ]
    
  def submit(self, task):
    system("bsub bash %s/task.sh -o %s/stdout.txt -e %s/stderr.txt" % (task.task_dir, task.task_dir, task.task_dir))
    
  def kill(self, task):
    raise Exception("bkill %s" % task.external_id)

job_queue = LocalQueue()
flock_home = os.path.dirname(os.path.realpath(__file__))
modified_env=dict(os.environ)
modified_env['FLOCK_HOME'] = flock_home

def run(run_id, script, args):
  if(os.path.exists(run_id)):
    print "\"%s\" already exists. Aborting." % run_id
    sys.exit(1)
  
  os.makedirs("%s/temp" % run_id)
  temp_run_script = "%s/temp/run_script.R"%run_id
  with open(temp_run_script, "w") as fd:
    fd.write("source('%s/spawn.R');\n" % flock_home)
    fd.write("source('%s');\n" % script)
  system("R --vanilla --args %s < %s" % (" ".join(args), temp_run_script))
  while True:
    submitted_count = submit_created(run_id)
    if submitted_count == 0:
      break

def submit_created(run_id):
  submitted_count = 0
  tasks = job_queue.find_tasks(run_id)
  for task in tasks:
    if task.status == CREATED:
      job_queue.submit(task)
      submitted_count += 1
  return submitted_count

def kill(run_id):
  tasks = job_queue.find_tasks(run_id)
  for task in tasks:
    if task.status == SUBMITTED:
      job_queue.kill(task)

#def check(run_id):
#  tasks = find_tasks(run_id)
#  for task in tasks:
    #if task.status == CREATED:
    #  submit(task)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--local', help='foo help', action='store_true')
  parser.add_argument('command', help='bar help')
  parser.add_argument('run_id', help='bar help')
  parser.add_argument('arg', nargs='*', help='bar help')
  
  args = parser.parse_args()
  
  #print args
  #print "flock_home=%s" % flock_home
  command = args.command
  modified_env['FLOCK_RUN_DIR'] = os.path.abspath(args.run_id)
  if command == "run":
    run(args.run_id, args.arg[0], args.arg[1:])
  elif command == "kill":
    kill(args.run_id)
  else:
    raise Exception("Unknown command: %s" % command)

#command = sys.argv[1]
#if command == "run":
