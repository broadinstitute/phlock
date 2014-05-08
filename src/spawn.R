# TODO: when submitting, need to check *.finished exists.  If so, delete it.

spawn <- function(inputs, task_script_name,  gather_script_name, gather_params=NULL, script_path=NULL, run_dir=NULL) {
	if(is.null(script_path)) {
		script_path = Sys.getenv("FLOCK_HOME");
		stopifnot(script_path != '');
	}

	if(is.null(run_dir)) {
		run_dir = Sys.getenv("FLOCK_RUN_DIR")
		stopifnot(run_dir != '');
	}
	
	created.jobs <- c()
	submit_command <- function(name, cmd) {
		fileConn <- file(name)
		writeLines(cmd, fileConn)
		close(fileConn)

		created.jobs <<- c(created.jobs, dirname(name))
	}

	id.fmt.str = sprintf("%%0%.0f.0f", log(length(inputs))/log(10));
	job_details = list()
	for(job.index in 1:length(inputs)) {
		job.id = sprintf(id.fmt.str, job.index);
		param = inputs[job.index];
		job_dir = paste(run_dir, '/jobs/', job.id, sep='');
		dir.create(job_dir, recursive=TRUE);
		input_file = paste(job_dir, '/input.Rdata', sep='')
		output_file = paste(job_dir, '/output.Rdata', sep='')
		script_name = task_script_name;
		completion_file = paste(job_dir, '/finished-time.txt', sep='')
		save(run_dir, job_dir, input_file, output_file, script_name, param, completion_file, file=input_file)
		submit_command(paste(job_dir, '/task.sh', sep=''), paste('exec ', script_path, '/execute_task.R ', input_file, sep=''))
		job_details[[length(job_details)+1]] = list(run_dir=run_dir, job_dir=job_dir, input_file=input_file, output_file=output_file, script_name=script_name, param=param)
	}

	dir.create(paste(run_dir, '/jobs/gather', sep=''), recursive=TRUE);
	gather_input_file = paste(run_dir, '/jobs/gather/input.Rdata', sep='')
	completion_file = paste(run_dir, '/jobs/gather/finished-time.txt', sep='')
	param = gather_params;
	script_name = gather_script_name;
	save(run_dir, job_dir, job_details, param, script_name, completion_file, file=gather_input_file)
	submit_command(paste(run_dir, '/jobs/gather/task.sh', sep=''), paste(script_path, '/execute_task.R ', gather_input_file, sep=''))

	# write the list of task scripts
	fileConn <- file(paste(run_dir, '/jobs/task_dirs.txt', sep=''))
	print(created.jobs);
	writeLines(created.jobs, fileConn)
	close(fileConn)
}
