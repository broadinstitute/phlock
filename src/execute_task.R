args <- commandArgs(TRUE);

# load the global variables
load(args[1]);

# load the per-task variables
load(args[2]);

# run the per-task script
source(flock_script_name);

# write out record that task completed successfully
fileConn<-file(flock_completion_file)
writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
close(fileConn)
