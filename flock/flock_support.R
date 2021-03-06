# TODO: when submitting, need to check *.finished exists.  If so, delete it.

flock.run <- function(inputs, 
  task_script_name=NULL, 
  gather_script_name=NULL, 
  flock_common_state=NULL,
  common=flock_common_state,
  script_path=NULL, 
  x_flock_run_dir=NULL, 
  task_function=NULL,
  gather_function=NULL,
  sources=NULL) {

  flock_common_state <- common

  if(is.null(script_path)) {
    script_path = flock_home
    stopifnot(script_path != '');
  }

  if(is.null(x_flock_run_dir)) {
#    flock_run_dir = flock_run_dir
    stopifnot(flock_run_dir != '');
  }

  task.dir <- 'tasks';
  task.dir.try <- 0
  full.task.dir <- paste(flock_run_dir, '/', task.dir, sep='')
  while(file.exists(full.task.dir)) {
    stopifnot(task.dir.try < 100)
    task.dir.try <- task.dir.try + 1
    task.dir <- paste('tasks-', task.dir.try, sep='')
    full.task.dir <- paste(flock_run_dir, '/', task.dir, sep='')
  }

  dir.create(full.task.dir, recursive=TRUE);

  flock_common_state_file = paste(flock_run_dir, '/',task.dir,'/flock_common_state.Rdata', sep='');
  env_file = paste(flock_run_dir, '/env.sh', sep='')
  flock_files_to_source <- c(paste(script_path, '/flock_support.R', sep=''), sources)
  save(flock_common_state, flock_files_to_source, flock_notify_command, flock_home, file=flock_common_state_file)
  
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
  if(exists("flock_test_job_count") && !is.na(flock_test_job_count)) {
    job.count = min(flock_test_job_count, job.count)
  }
  for(job.index in 1:job.count) {
    job.id = sprintf(id.fmt.str, job.index);
    job.subdir = job.id
    if(nchar(job.id) > 3) {
      job.subdir = paste(substr(job.subdir, 1, nchar(job.subdir)-3), '/', job.id, sep='')
    }
    flock_per_task_state = inputs[[job.index]];
    flock_job_dir = paste(flock_run_dir, '/', task.dir, '/', job.subdir, sep='');
    dir.create(flock_job_dir, recursive=TRUE);
    flock_input_file = paste(flock_job_dir, '/input.Rdata', sep='')
    flock_output_file = paste(flock_job_dir, '/output.Rdata', sep='')
    flock_script_name = task_script_name;
    flock_function_name = task_function;
    flock_completion_file = paste(flock_job_dir, '/finished-time.txt', sep='')
    flock_starting_file = paste(flock_job_dir, '/started-time.txt', sep='')
    save(flock_function_name, flock_starting_file, flock_run_dir, flock_job_dir, flock_input_file, flock_output_file, flock_script_name, flock_per_task_state, flock_completion_file, file=flock_input_file)
    submit_command('1', paste(job.subdir, '/task.sh', sep=''), paste('set -ex\nsource ',env_file,'\nexec R --vanilla --args ', script_path, ' ', flock_common_state_file, ' ', flock_input_file, ' < ', script_path, '/execute_task.R', sep=''))
    flock_job_details[[length(flock_job_details)+1]] = list(flock_run_dir=flock_run_dir, flock_job_dir=flock_job_dir, flock_input_file=flock_input_file, flock_output_file=flock_output_file, flock_script_name=flock_script_name, flock_per_task_state=flock_per_task_state)
  }

  if(!is.null(gather_script_name) || !is.null(gather_function)) {
    dir.create(paste(flock_run_dir, '/',task.dir,'/gather', sep=''), recursive=TRUE);
    gather_input_file = paste(flock_run_dir, '/',task.dir,'/gather/input.Rdata', sep='')
    flock_completion_file = paste(flock_run_dir, '/',task.dir,'/gather/finished-time.txt', sep='')
    flock_starting_file = paste(flock_run_dir, '/',task.dir,'/gather/started-time.txt', sep='')
    flock_per_task_state = flock_job_details;
    flock_script_name = gather_script_name;
    flock_function_name = gather_function;
    save(flock_function_name, flock_starting_file, flock_run_dir, flock_job_dir, flock_per_task_state, flock_script_name, flock_completion_file, file=gather_input_file)
    submit_command('2', 'gather/task.sh', paste('set -ex\nsource ',env_file,'\nexec R --vanilla --args ', script_path, ' ', flock_common_state_file, ' ', gather_input_file, ' < ', script_path, '/execute_task.R', sep=''))
  }

  # write the list of task scripts
  taskset.file <- paste(flock_run_dir, '/',task.dir,'/task_dirs.txt', sep='')
  fileConn <- file(taskset.file)
  #print(created.jobs);
  #print(unlist(created.jobs));
  writeLines(unlist(created.jobs), fileConn)
  close(fileConn)

  # create the other default directories
  temp.dir <- paste(flock_run_dir, "/temp", sep='')
  results.dir <- paste(flock_run_dir, "/results", sep='')
  dir.create(temp.dir)
  dir.create(results.dir)

  if(!is.null(flock_notify_command)) {
    ret.code <- system(paste(flock_notify_command, " taskset ", flock_run_dir, " ", taskset.file, sep=''))
    stopifnot(ret.code == 0)
  }
}
