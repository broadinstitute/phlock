a = 0

str(flock_per_task_state);
print("filenames");
print(flock_per_task_state);
for(d in flock_per_task_state) {
	print("d");
	print(d);
	load(d$flock_output_file)
	a = a + squared
}

fileConn<-file(paste(flock_run_dir, "/result.txt", sep=''))
writeLines(sprintf("%f", a), fileConn)
close(fileConn)
