
#     run_dir
#     job_dir
#     input_file
#     output_file
#     status_file - where to write final status upon successful completion.  based on input_file
#     script_name
#     r_path

stopifnot(flock_per_task_state == 1)
squared = flock_per_task_state ** 2
save(squared, file=flock_output_file)
