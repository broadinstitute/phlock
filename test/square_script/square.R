
%     run_dir
%     job_dir
%     input_file
%     output_file
%     status_file - where to write final status upon successful completion.  based on input_file
%     script_name
%     r_path

param = param ** 2
save(squared, file=output_file)
