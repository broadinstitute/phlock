# flock

This is a lightweight library for executing map/reduce style jobs.  (Perhaps "scatter/gather" is a better description.)

More specifically, in this library can be used to execute an R script on a vector of inputs, potentially submitting them to a queuing system where they'll execute asynchronously, and then 
aggregate all of the results with a second R script.

All state is coordinated on the filesystem under the following directory structure:

```
/xchip/datasci/runs/[ID]
/xchip/datasci/runs/[ID]/jobs/[JOBID]/stdout.txt
/xchip/datasci/runs/[ID]/jobs/[JOBID]/stderr.txt
/xchip/datasci/runs/[ID]/jobs/[JOBID]/task.sh
/xchip/datasci/runs/[ID]/jobs/job_it_map.txt
/xchip/datasci/runs/[ID]/jobs/log/[JOBID].txt
/xchip/datasci/runs/[ID]/jobs/input/[JOBID]
/xchip/datasci/runs/[ID]/jobs/output/[JOBID]
```

## A second attempt

I've written things like this before integrated in large systems, and from those experiences I've learned lessons and made the following key changes:

1. There is no demon monitoring all the jobs for completion.   The monitoring of jobs is left to the job submission framework and bothering to duplicate that functionality.
2. There is no master process doing all of the job submissions.  This allows any user to submit jobs to the native (LSF) queue under their own account.  Jobs will run as the actual users avoiding problems with role accounts submitting lots of jobs, and file permissions.
3. This is an independant library with minimal dependancies.  In the past, I had put such scatter/gather code embedded in bigger projects.  This meant it was great for large production workflows, but carried a lot of baggage for other people that might want to leverage the higher level API this provides.
4. Execution of scripts is kept simple.   In the past there was a fair amount of magic around piping data in and serializing data out.  Instead, this round, I'm trying to make all interactions simple to make it easier to debug.

## State that needs being captured

the task id -> submission id mapping
task id -> task id (dependancy mapping)
next job-dir for run-id

Write these to files?
Update on resubmit?

### Example

```R
spawn(task_script, inputs, aggregate_script)
```

```R
spawn('square.R', c(1,2,3), 'aggregate.R')
```

The spawn method will return immediately after the jobs have been queued for execution.  There is no guarentee that anything has been run by the time the function has returned.

Spawn only writes out all of the files that are needed.

Each task 

# To run a script

flock start [options] run-id script arg1 arg2 ...

options can be:
  LSF options:
    -P project to give to LSF
    -q queue to use

  --local execute locally

Should give an error if run-id exists

# To check if script is done

flock check run-id

# To wait until script is done

flock waitfor run-id

# To take any failed jobs and resubmit them for execution again.

flock resubmit run-id

# To stop jobs

flock kill run-id

flock poll run-id
# generates list of task ids by walking the directory tree
# tasks get assigned one of three states: CREATED (on disk) -> SUBMITTED (in LSF queue) -> FINISHED (marked as done)
All tasks which are in state CREATED, submit to LSF

