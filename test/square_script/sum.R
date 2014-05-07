filenames = param
a = 0

for(filename in filenames) {
	load(filename)
	a = a + squared
}

fileConn<-file(paste(run_dir, "/result.txt", sep=''))
writeLines(a, fileConn)
close(fileConn)
