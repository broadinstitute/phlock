submit_to_lsf <- function(command, stdout_file, stderr_file) {
	cmd <- sprintf("bsub %s -o %s -e %s", command, stdout_file, stderr_file);
	print(cmd);
}

submit_locally <- function(command, stdout_file, stderr_file) {
	cmd <- sprintf("%s > %s 2> %s", command, stdout_file, stderr_file);
	print(cmd);
}

spawn <- function(build_path, run_dir, inputs, task_script_name, gather_params, gather_script_name, submit_command) {
	id.fmt.str = sprintf("%%0%.0f.0f", log(length(inputs))/log(10));
	job_details = c()
	for(job.index in 1:length(inputs)) {
		job.id = sprintf(id.fmt.str, job.index);
		param = inputs[job.index];
		job_dir = paste(run_dir, '/jobs/', job.id, sep='');
		dir.create(job_dir, recursive=TRUE);
		input_file = paste(job_dir, '/input.Rdata', sep='')
		output_file = paste(job_dir, '/output.Rdata', sep='')
		stdout_file = paste(job_dir, '/stdout.txt', sep='')
		stderr_file = paste(job_dir, '/stderr.txt', sep='')
		script_name = task_script_name;
		save(run_dir, job_dir, input_file, output_file, script_name, param, file=input_file)
		submit_command(paste(build_path, '/execute_task.R ', input_file, sep=''), stdout_file, stderr_file)
		job_details = c(job_details, list(run_dir=run_dir, job_dir=job_dir, input_file=input_file, output_file=output_file, script_name=script_name, param=param))
	}
	gather_input_file = paste(run_dir, '/jobs/gather.Rdata',sep='')
	stdout_file = paste(run_dir, '/jobs/gather.stdout',sep='')
	stderr_file = paste(run_dir, '/jobs/gather.stderr',sep='')
	param = gather_params;
	script_name = gather_script_name;
	save(run_dir, job_dir, job_details, param, script_name, file=gather_input_file)
	submit_command(paste(build_path, '/execute_task.R ', gather_input_file, sep=''), stdout_file, stderr_file)
}

spawn(".", "temp", c(1,2,3,4,5), "go.R", c(), "gather.R", submit_to_lsf);
