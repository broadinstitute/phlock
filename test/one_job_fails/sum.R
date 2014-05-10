a = 0

str(job_details);
print("filenames");
print(job_details);
for(d in job_details) {
	print("d");
	print(d);
	load(d$output_file)
	a = a + squared
}

fileConn<-file(paste(run_dir, "/result.txt", sep=''))
writeLines(sprintf("%f", a), fileConn)
close(fileConn)
