stopifnot(flock_run_dir != NULL)
stopifnot(flock_job_dir != NULL)
stopifnot(flock_input_file != NULL)
stopifnot(flock_output_file != NULL)
stopifnot(flock_script_name != NULL)
stopifnot(flock_per_task_state != NULL)
stopifnot(flock_common_state != NULL)

squared = flock_per_task_state ** 2
save(squared, file=flock_output_file)
