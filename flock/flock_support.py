def flock_run(inputs, task_script_name, flock_settings, gather_script_name=None, flock_common_state=None):
  python_path = flock_settings["python_path"]
  flock_home = flock_settings["flock_home"]
  flock_run_dir = flock_settings["flock_run_dir"]
  script_path = flock_home

  task_dir <- 'tasks'
  
  os.makedirs(os.path.join(flock_run_dir, task_dir))
  flock_common_state_file = os.path.join(flock_run_dir,task.dir,'flock_common_state.pickle')
  with open(flock_common_state_file, "w") as fd:
    pickle.save(fd, flock_common_state)
  
  id_fmt_str = "%%0%.0f.0f" % (math.ceil(math.log(len(inputs))/math.log(10)))
  flock_job_details = []
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
    os.makedirs(flock_job_dir)
    flock_input_file = os.path.join(flock_job_dir, "input.pickle")
    flock_output_file = os.path.join(flock_job_dir, "output.pickle")
    flock_script_name = task_script_name
    flock_completion_file = os.path.join(flock_job_dir, 'finished-time.txt')
    flock_starting_file = os.path.join(flock_job_dir, "started-time.txt")
    state = flock_starting_file, flock_run_dir, flock_job_dir, flock_input_file, flock_output_file, flock_script_name, flock_per_task_state, flock_completion_file
    pickle.save(state)
    submit_command("1", os.path.join(job_subdir, "task.sh"), "exec %s %s %s %s" % (python_path, execute_task_path, script_path, flock_common_state_file, flock_input_file))
    job_details.append(dict(flock_run_dir=flock_run_dir, flock_job_dir=flock_job_dir, flock_input_file=flock_input_file, flock_output_file=flock_output_file, flock_script_name=flock_script_name, flock_per_task_state=flock_per_task_state))
  
  if gather_script_name != None:
    os.makedirs(os.path.join(flock_run_dir, task_dir, "gather"))
    gather_input_file = os.path.join(flock_run_dir, task_dir, "gather", "input.pickle")
    flock_completion_file = os.path.join(flock_job_dir, 'finished-time.txt')
    flock_starting_file = os.path.join(flock_job_dir, "started-time.txt")
    flock_per_task_state = flock_job_details;
    flock_script_name = gather_script_name;
    save(flock_starting_file, flock_run_dir, flock_job_dir, flock_per_task_state, flock_script_name, flock_completion_file, file=gather_input_file)
    submit_command('2', 'gather/task.sh', "exec %s %s %s %s" % (python_path, execute_task_path, script_path, flock_common_state_file, flock_input_file))
        
  # write the list of task scripts
  taskset_file = os.path.join(flock_run_dir, task_dir, "task_dirs.txt")
  with open(taskset_file, "w") as fd:
    for line in created_jobs:
      fd.write("\t".join(line) + "\n")
  
  if flock_settings["flock_notify_command"] != None:
    subprocess.check_call("%s taskset %s %s" % (flock_settings["flock_notify_command"], flock_run_dir, taskset_file))
  