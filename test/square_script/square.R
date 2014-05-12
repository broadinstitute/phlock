stopifnot(run_dir != NULL)
stopifnot(job_dir != NULL)
stopifnot(param != NULL)
stopifnot(input_file != NULL)
stopifnot(output_file != NULL)
stopifnot(script_name != NULL)
stopifnot(completion_file != NULL)

squared = param ** 2
save(squared, file=output_file)
