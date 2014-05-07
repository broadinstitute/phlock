
%     run_dir
%     job_dir
%     input_file
%     output_file
%     status_file - where to write final status upon successful completion.  based on input_file
%     script_name
%     r_path

system("sleep 5")
param = param ** 2
save(param, file=output_file)
