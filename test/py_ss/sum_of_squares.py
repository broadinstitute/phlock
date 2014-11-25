import pickle
from flock_support import flock_run

def do(values):
  flock_run(values, ["test/py_ss"], "sum_of_squares:square", gather_function_name="sum_of_squares:sum")

def square(common_state, flock_per_task_state):
  assert(flock_per_task_state['flock_run_dir'] != None)
  assert(flock_per_task_state['flock_job_dir'] != None)
  assert(flock_per_task_state['flock_input_file'] != None)
  assert(flock_per_task_state['flock_output_file'] != None)
  assert(flock_per_task_state['flock_per_task_state'] != None)

  squared = flock_per_task_state['flock_per_task_state'] ** 2
  with open(flock_per_task_state['flock_output_file'], "w") as fd:
    pickle.dump(squared, fd)

def sum(common_state, per_task_state):
  print "done"

