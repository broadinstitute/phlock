  assert(flock_run_dir != None)
  assert(flock_job_dir != None)
  assert(flock_input_file != None)
  assert(flock_output_file != None)
  assert(flock_script_name != None)
  assert(flock_per_task_state != None)
  assert(flock_common_state != None)

  squared = flock_per_task_state ** 2
  save(squared, file=flock_output_file)
  assert(flock_run_dir != None)
  assert(flock_job_dir != None)
  assert(flock_input_file != None)
  assert(flock_output_file != None)
  assert(flock_script_name != None)
  assert(flock_per_task_state != None)
  assert(flock_common_state != None)

  squared = flock_per_task_state ** 2
  save(squared, file=flock_output_file)
