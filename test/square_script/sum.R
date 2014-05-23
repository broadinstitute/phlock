a = 0

str(flock_job_details);
print("filenames");
print(flock_job_details);
for(d in flock_job_details) {
	print("d");
	print(d);
	load(d$flock_output_file)
	a = a + squared
}

fileConn<-file(paste(flock_run_dir, "/result.txt", sep=''))
writeLines(sprintf("%f", a), fileConn)
close(fileConn)
