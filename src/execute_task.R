#!/usr/bin/Rscript --vanilla --default-packages=utils

args <- commandArgs(TRUE);
load(args[1]);
source(script_name);

fileConn<-file(paste(args[1], '.completed', sep=''))
writeLines(c("Hello","World"), fileConn)
close(fileConn)
