import sys
import pickle
import time

this_script, py_flock_state_file, common_state_file, per_task_state_file = sys.argv

with open(py_flock_state_file) as fd:
  scripts = pickle.load(fd)

with open(common_state_file) as fd:
  common_state = pickle.load(fd)
  
with open(per_task_state_file) as fd:
  per_task_state = pickle.load(fd)

# find the function to invoke
sys.path.extend(scripts["path"])
module = __import__(scripts['module_name'])
task_function = getattr(module, scripts['function_name'])

print per_task_state

with open(per_task_state['flock_starting_file'], 'w') as fd:
  fd.write(time.strftime('%a %b %d %X %Y', time.localtime()))

task_function(common_state, per_task_state)

# write out record that task completed successfully
with open(per_task_state['flock_completion_file'], 'w') as fd:
  fd.write(time.strftime('%a %b %d %X %Y', time.localtime()))
