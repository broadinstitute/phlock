import os
import math
import pickle

global_flock_settings = None

def create_if_missing(dir_name):
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)

def flock_run(inputs, module_path, task_function_name, flock_settings=None, gather_function_name=None, flock_common_state=None):
  if flock_settings == None:
    flock_settings = global_flock_settings

  python_path = flock_settings["python_path"]
  flock_home = flock_settings["flock_home"]
  flock_run_dir = flock_settings["flock_run_dir"]
  script_path = flock_home
  execute_task_path = os.path.join(flock_home, "execute_task.py")

  task_dir = 'tasks'
  
  create_if_missing(os.path.join(flock_run_dir, task_dir))
  # write out how to find the function to invoke
  per_task_pyflock_file = os.path.join(flock_run_dir,task_dir,'pyflock_script_state.pickle')
  module_name, function_name = task_function_name.split(":")
  per_task_pyflock = dict(path=module_path, module_name=module_name, function_name=function_name)
  with open(per_task_pyflock_file, "w") as fd:
    pickle.dump(per_task_pyflock, fd)
  
  # write out common state
  flock_common_state_file = os.path.join(flock_run_dir,task_dir,'flock_common_state.pickle')
  with open(flock_common_state_file, "w") as fd:
    pickle.dump(flock_common_state, fd)
  
  id_fmt_str = "%%0%.0f.0f" % (math.ceil(math.log(len(inputs))/math.log(10)))
  flock_job_details = []
  created_jobs = []
  def submit_command(group, name, cmd):
#    full_task_dir = 
    with open(os.path.join(flock_run_dir, task_dir, name), "w") as fd:
      fd.write(cmd)
    
    created_jobs.append( (group, os.path.join(task_dir, os.path.dirname(name))))

  job_count = len(inputs)
  if flock_settings["flock_test_job_count"] != None:
    job_count = min(flock_settings["flock_test_job_count"], job_count)
  for job_index in xrange(job_count):
    job_id = id_fmt_str % job_index
    job_subdir = job_id
    if len(job_id) > 3:
      job_subdir = os.path.join(job_subdir[:-3], job_id)
    flock_per_task_state = inputs[job_index]
    flock_job_dir = os.path.join(flock_run_dir, task_dir, job_subdir)
    create_if_missing(flock_job_dir)
    flock_input_file = os.path.join(flock_job_dir, "input.pickle")
    flock_output_file = os.path.join(flock_job_dir, "output.pickle")
    flock_completion_file = os.path.join(flock_job_dir, 'finished-time.txt')
    flock_starting_file = os.path.join(flock_job_dir, "started-time.txt")
#    state = flock_starting_file, flock_run_dir, flock_job_dir, flock_input_file, flock_output_file, flock_per_task_state, flock_completion_file
    state = dict(flock_run_dir=flock_run_dir, flock_job_dir=flock_job_dir, flock_input_file=flock_input_file, flock_output_file=flock_output_file, flock_per_task_state=flock_per_task_state,
          flock_starting_file=    flock_starting_file,     flock_completion_file =     flock_completion_file)
    with open(flock_input_file, "w") as fd:
      pickle.dump(state, fd)
    submit_command("1", os.path.join(job_subdir, "task.sh"), "exec %s %s %s %s %s" % (python_path, execute_task_path, per_task_pyflock_file, flock_common_state_file, flock_input_file))
    flock_job_details.append(state)
  
  if False and gather_function_name != None:
    create_if_missing(os.path.join(flock_run_dir, task_dir, "gather"))
    gather_input_file = os.path.join(flock_run_dir, task_dir, "gather", "input.pickle")
    flock_completion_file = os.path.join(flock_job_dir, 'finished-time.txt')
    flock_starting_file = os.path.join(flock_job_dir, "started-time.txt")
    flock_per_task_state = flock_job_details;
    flock_script_name = gather_script_name;
    state = flock_starting_file, flock_run_dir, flock_job_dir, flock_per_task_state, flock_script_name, flock_completion_file
    with open(gather_input_file, "w") as fd:
      pickle.dump(state, fd)
    submit_command('2', 'gather/task.sh', "exec %s %s %s %s %s" % (python_path, execute_task_path, gather_pyflock_file, flock_common_state_file, flock_input_file))
        
  # write the list of task scripts
  taskset_file = os.path.join(flock_run_dir, task_dir, "task_dirs.txt")
  with open(taskset_file, "w") as fd:
    for line in created_jobs:
      fd.write(" ".join(line) + "\n")
  
  if flock_settings["flock_notify_command"] != None:
    subprocess.check_call("%s taskset %s %s" % (flock_settings["flock_notify_command"], flock_run_dir, taskset_file))
  