# flock

This is a lightweight library for executing map/reduce style jobs.  (Perhaps "scatter/gather" is a better description.)

More specifically, in this library can be used to execute an R script on a vector or list of inputs, potentially submitting them to a queuing system where they'll execute asynchronously, and then 
aggregate all of the results with a second R script.

There are three types of R-scripts invoked:

1. Top level script: The first script invoked which creates the other tasks.   This script will have the following variables defined before it executes: "flock_run_dir"
2. Per-task script: The script executed per each task.  This script will have the following variable defined: "flock_run_dir", "flock_job_dir", "flock_input_file", 
"flock_output_file", "flock_script_name", "flock_per_task_state", "flock_common_state"
3. Gather script: The final script executed after all jobs have completed successful.  This will have the following variables defined: "flock_run_dir", "flock_job_dir", 
"flock_script_name", "flock_common_state" and "flock_job_details"

All state is coordinated on the filesystem under the following directory structure:

```
The root directory            [run_id]

Directory holding all tasks   [run_id]/tasks

List of all tasks             [run_id]/tasks/task_dirs.txt

A directory per task          [run_id]/tasks/[task_id]

If this file exists this task [run_id]/tasks/[task_id]/finished-time.txt 
completed successful.

The per-task state for a      [run_id]/tasks/[task_id]/input.Rdata       
task.

The execution engine id for   [run_id]/tasks/[task_id]/job_id.txt        
the given task.

The serialized output for     [run_id]/tasks/[task_id]/output.Rdata      
the task which is passed to
the gather script.

The stderr captured from the  [run_id]/tasks/[task_id]/stderr.txt        
task.

The stdout captured from the  [run_id]/tasks/[task_id]/stdout.txt  
task.

The script that was executed  [run_id]/tasks/[task_id]/task.sh
for this task.
```

"run_id" is chosen when the "flock" command is run.  "task_id" is assigned numerically for each task, and there'll be one additional task named "gather" run after all other tasks.

The "run_id" is also interpreted as the config file which specifies what exactly should be run.  (The syntax of the file is YAML)  For example, lets 
assume we have a basic config file named "sample" with the following content:
```
base_run_dir: /home/pgm/runs
executor: lsf
bsub_options: "-q long"
invoke: |
  source("code.R")
  run.calculation.over(seq(5))
```

The "base_run_dir" is the root directory that all files related to the run will be stored.
"executor" is the execution engine to talk to.  Values can be "lsf", "sge", "local", or "localbg"
"bsubOptions" are extra options to specify when submitting jobs via lsf
"invoke" is the R code to execute.  Instead run.calculation.over, a call to flock.spawn will be made which actually creates the jobs.  It's a good practice to only have the parameters you want to run your anaylsis defined in this file and use source to load anything else you depend on.

Runing this script will create a directory named /home/pgm/runs/sample, where all the results will go.

Common settings can be placed in a ~/.flock config file and overridden in the config file specified as a run-id.

## A second attempt

I've written job-management systems like this before integrated in large systems, and from those experiences I've learned lessons and made the following simplifying key changes:

1. There is no demon monitoring all the jobs for completion.   The monitoring of jobs is left to the job submission framework and bothering to duplicate that functionality.
2. There is no master process doing all of the job submissions.  This allows any user to submit jobs to the native (LSF) queue under their own account.  Jobs will run as the actual users avoiding problems with role accounts submitting lots of jobs, and file permissions issues.
3. This is an independant library with minimal dependancies.  In the past, I had put such scatter/gather code embedded in bigger projects.  This meant it was great for large production workflows, but carried a lot of baggage for other people that might want to leverage the higher level API this provides.
4. Execution of scripts is kept simple.   In the past there was a fair amount of magic around piping data in and serializing data out.  Instead, this round, I'm trying to make all interactions simple to make it easier to debug.

### Example

```R
spawn(task_script, inputs, aggregate_script)
```

```R
spawn('square.R', c(1,2,3), 'aggregate.R')
```

The spawn method will return immediately after the jobs have been queued for execution.  There is no guarentee that anything has been run by the time the function has returned.

Spawn only populates the run_dir with all the files that are needed.  After the top-level R script completes, flock will start submitting all of those tasks using which ever
execution engine is configured.

# To start a run.  By default, will wait for all tasks to complete.  Can be safely killed after all tasks have been submitted.
flock run run-id

If the directory for the run already exists, it will abort to prevent overwriting existing files.

# To print the state of all tasks which make up this task
flock check run-id

# To take any failed jobs and resubmit them for execution again.
flock retry run-id

# To kill all running tasks associated with a run
flock kill run-id

# To monitor (will print task states until all tasks are done) a run
flock poll run-id

# tasks get assigned one of three states: 
1. Created (this task exists only on disk)
2. Submitting (this task has been submitted to the execution engine)
3. Finished (this task successfully completed)
4. Failed (this task was submitted but never successfully completed and is no longer in the execution engine's queue.)
5. Waiting (this task is waiting for other tasks to complete before it starts)

