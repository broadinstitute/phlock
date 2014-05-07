import sys
import collections

CREATED = "Created"
SUBMITTED = "Submitted"
FINISHED = "Finished"

Task = collections.named_tuple("Task", ["task_dir", "external_id", "status"])

def read_task_dirs(run_id):

def read_task_dependancies(run_id):

def is_finished(run_id, task_dir):

class LocalQueue(object):
  def find_tasks(self, run_id):
    task_dirs = read_task_dirs(run_id)
    return [ Task(task_dir, None, [], FINISHED if is_finished(run_id, task_dir) else CREATED ]
    
  def submit(self, task):
    os.system("%s > %s/stdout.txt 2> %s/stderr.txt" % (task.executable, task.task_dir, task.task_dir))
    
  def kill(self, task):
    raise Exception("not implemented")

class LsfQueue(object):
  def get_active_lsf_jobs(self):
  
  def read_task_external_ids(self, run_id):
  
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
        
    return [ Task(task_dir, get_external_id(task_dir), get_status(task_dir) ]
    
  def submit(self, task):
    os.system("bsub %s -o %s/stdout.txt -e %s/stderr.txt" % (task.executable, task.task_dir, task.task_dir))
    
  def kill(self, task):
    raise Exception("bkill %s" % task.external_id)

job_queue = LocalQueue()

def run(run_id, script, args):
  os.system("R CMD BATCH %s %s" % script, " ".join(args))
  while True:
    submitted_count = poll(run_id)
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

command = sys.argv[1]

if command == "run":
