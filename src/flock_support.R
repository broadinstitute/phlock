# TODO: when submitting, need to check *.finished exists.  If so, delete it.

flock.run <- function(inputs, task_script_name, gather_script_name, shared_params=NULL, gather_params=NULL, script_path=NULL, run_dir=NULL) {
	if(is.null(script_path)) {
		script_path = Sys.getenv("FLOCK_HOME");
		stopifnot(script_path != '');
	}

	if(is.null(run_dir)) {
		run_dir = Sys.getenv("FLOCK_RUN_DIR")
		stopifnot(run_dir != '');
	}

	dir.create(paste(run_dir, '/jobs', sep=''), recursive=TRUE);
	shared_params_file = paste(run_dir, '/jobs/shared_params.Rdata', sep='');
	save(shared_params, file=shared_params_file)
	
	created.jobs <- list()
	submit_command <- function(group, name, cmd) {
		fileConn <- file(paste(run_dir, '/jobs/', name, sep=''))
		writeLines(cmd, fileConn)
		close(fileConn)
    
    created.jobs[[length(created.jobs)+1]] = paste(group, ' ', paste('jobs/', dirname(name), sep=''), sep='')
    created.jobs <<- created.jobs
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
		submit_command('1', paste(job.id, '/task.sh', sep=''), paste('exec R --vanilla --args ', shared_params_file, ' ', input_file, ' < ', script_path, '/execute_task.R', sep=''))
		job_details[[length(job_details)+1]] = list(run_dir=run_dir, job_dir=job_dir, input_file=input_file, output_file=output_file, script_name=script_name, param=param)
	}

	dir.create(paste(run_dir, '/jobs/gather', sep=''), recursive=TRUE);
	gather_input_file = paste(run_dir, '/jobs/gather/input.Rdata', sep='')
	completion_file = paste(run_dir, '/jobs/gather/finished-time.txt', sep='')
	param = gather_params;
	script_name = gather_script_name;
	save(run_dir, job_dir, job_details, param, script_name, completion_file, file=gather_input_file)
	submit_command('2', 'gather/task.sh', paste(script_path, '/execute_task.R ', shared_params_file, ' ', gather_input_file, sep=''))

	# write the list of task scripts
	fileConn <- file(paste(run_dir, '/jobs/task_dirs.txt', sep=''))
	#print(created.jobs);
	#print(unlist(created.jobs));
	writeLines(unlist(created.jobs), fileConn)
	close(fileConn)
}
