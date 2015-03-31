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

# run the per-task script
source(flock_script_name);

# write out record that task completed successfully
fileConn<-file(flock_completion_file)
writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
close(fileConn)
if(file.info(flock_completion_file)$size <= 0) {
  unlink(flock_completion_file)
  stop("Filesystem filled up?")
}

