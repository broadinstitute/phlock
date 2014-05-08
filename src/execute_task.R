#!/usr/bin/Rscript --vanilla --default-packages=utils

args <- commandArgs(TRUE);
load(args[1]);
source(script_name);

fileConn<-file(completion_file)
writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
close(fileConn)
