args <- commandArgs(TRUE);

# load the global variables
if(args[1] != "NULL") {
  load(args[1]);
}

# load the per-task variables
if(args[2] != "NULL") {
  load(args[2]);
}

fileConn<-file(flock_starting_file)
writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
close(fileConn)
# this is a sign that the filesystem ran out of space.  R does not appear to catch this.
stopifnot(file.info(flock_starting_file)$size > 0)

# source all the required files
for(fn in flock_files_to_source) {
  source(fn)
}

# run the per-task script
if(!is.null(flock_function_name)) {
  temp.dir <- paste(flock_run_dir, "/temp", sep='')
  results.dir <- paste(flock_run_dir, "/results", sep='')
  task.dir <- flock_job_dir
  run <- list(results=results.dir, dir=flock_run_dir, task=task.dir)
  environment()[[flock_function_name]](per.task.state=flock_per_task_state, common.state=flock_common_state, output.file=flock_output_file, run=run)
}

if(!is.null(flock_script_name)) {
  source(flock_script_name);
}

# write out record that task completed successfully
fileConn<-file(flock_completion_file)
writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
close(fileConn)
if(file.info(flock_completion_file)$size <= 0) {
  unlink(flock_completion_file)
  stop("Filesystem filled up?")
}

