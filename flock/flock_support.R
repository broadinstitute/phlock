# TODO: when submitting, need to check *.finished exists.  If so, delete it.

flock.run <- function(inputs, task_script_name, gather_script_name=NULL, flock_common_state=NULL, script_path=NULL, flock_run_dir=NULL, flock_test_jobcount=NULL) {
  if(is.null(script_path)) {
    script_path = Sys.getenv("FLOCK_HOME");
    stopifnot(script_path != '');
  }

  if(is.null(flock_run_dir)) {
    flock_run_dir = Sys.getenv("FLOCK_RUN_DIR")
    stopifnot(flock_run_dir != '');
  }

  if(is.null(flock_test_jobcount)) {
    flock_test_jobcount = as.numeric(Sys.getenv("FLOCK_TEST_JOBCOUNT"))
  }

  task.dir <- 'tasks';

  dir.create(paste(flock_run_dir, '/', task.dir, sep=''), recursive=TRUE);
  flock_common_state_file = paste(flock_run_dir, '/',task.dir,'/flock_common_state.Rdata', sep='');
  save(flock_common_state, file=flock_common_state_file)
  
  created.jobs <- list()
  submit_command <- function(group, name, cmd) {
    fileConn <- file(paste(flock_run_dir, '/', task.dir, '/', name, sep=''))
    writeLines(cmd, fileConn)
    close(fileConn)
    
    created.jobs[[length(created.jobs)+1]] = paste(group, ' ', paste(task.dir, '/', dirname(name), sep=''), sep='')
    created.jobs <<- created.jobs
  }

  id.fmt.str = sprintf("%%0%.0f.0f", ceiling(log(length(inputs))/log(10)));
  flock_job_details = list()
  job.count = length(inputs);
  if(!is.na(flock_test_jobcount)) {
    job.count = min(flock_test_jobcount, job.count)
  }
  for(job.index in 1:job.count) {
    job.id = sprintf(id.fmt.str, job.index);
    job.subdir = job.id
    if(nchar(job.id) > 3) {
      job.subdir = paste(substr(job.subdir, 1, nchar(job.subdir)-3), '/', job.id, sep='')
    }
    flock_per_task_state = inputs[job.index];
    flock_job_dir = paste(flock_run_dir, '/', task.dir, '/', job.subdir, sep='');
    dir.create(flock_job_dir, recursive=TRUE);
    flock_input_file = paste(flock_job_dir, '/input.Rdata', sep='')
    flock_output_file = paste(flock_job_dir, '/output.Rdata', sep='')
    flock_script_name = task_script_name;
    flock_completion_file = paste(flock_job_dir, '/finished-time.txt', sep='')
    flock_starting_file = paste(flock_job_dir, '/started-time.txt', sep='')
    save(flock_starting_file, flock_run_dir, flock_job_dir, flock_input_file, flock_output_file, flock_script_name, flock_per_task_state, flock_completion_file, file=flock_input_file)
    submit_command('1', paste(job.subdir, '/task.sh', sep=''), paste('exec R --vanilla --args ', flock_common_state_file, ' ', flock_input_file, ' < ', script_path, '/execute_task.R', sep=''))
    flock_job_details[[length(flock_job_details)+1]] = list(flock_run_dir=flock_run_dir, flock_job_dir=flock_job_dir, flock_input_file=flock_input_file, flock_output_file=flock_output_file, flock_script_name=flock_script_name, flock_per_task_state=flock_per_task_state)
  }

  if(!is.null(gather_script_name)) {
    dir.create(paste(flock_run_dir, '/',task.dir,'/gather', sep=''), recursive=TRUE);
    gather_input_file = paste(flock_run_dir, '/',task.dir,'/gather/input.Rdata', sep='')
    flock_completion_file = paste(flock_run_dir, '/',task.dir,'/gather/finished-time.txt', sep='')
    flock_starting_file = paste(flock_run_dir, '/',task.dir,'/gather/started-time.txt', sep='')
    flock_per_task_state = flock_job_details;
    flock_script_name = gather_script_name;
    save(flock_starting_file, flock_run_dir, flock_job_dir, flock_per_task_state, flock_script_name, flock_completion_file, file=gather_input_file)
    submit_command('2', 'gather/task.sh', paste('exec R --vanilla --args ', flock_common_state_file, ' ', gather_input_file, ' < ', script_path, '/execute_task.R', sep=''))
  }

  # write the list of task scripts
  fileConn <- file(paste(flock_run_dir, '/',task.dir,'/task_dirs.txt', sep=''))
  #print(created.jobs);
  #print(unlist(created.jobs));
  writeLines(unlist(created.jobs), fileConn)
  close(fileConn)
}
