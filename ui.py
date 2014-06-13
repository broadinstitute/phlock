import time
import uuid
import flask
from flask import request
import threading
import subprocess
import pyte
import pty
import os
#import termios

import boto
import collections
import ConfigParser, os
import boto.ec2

config = ConfigParser.ConfigParser()
config.read(os.path.expanduser('~/.starcluster/config'))

AWS_ACCESS_KEY_ID = config.get("aws info", "AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws info", "AWS_SECRET_ACCESS_KEY")

terminals = {}

instance_sizes = [ ("c3.large", 2), ("c3.xlarge", 4), ("c3.2xlarge", 8), ("c3.4xlarge", 16), ("c3.8xlarge", 32) ]
instance_sizes.sort(lambda a, b: -cmp(a[1], b[1]))
cpus_per_instance = {}
for instance_type, cpus in instance_sizes:
  cpus_per_instance[instance_type] = cpus
cpus_per_instance['m3.medium'] = 1

CLUSTER_NAME="c"

c = boto.ec2.connection.EC2Connection(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def get_instance_counts():
  instances = c.get_only_instances()
  counts = collections.defaultdict(lambda: 0)
  for i in instances:
      counts[i.instance_type] += 1

  return counts

class Terminal(object):
  def __init__(self, id, proc, stdout, title):
    self.id = id
    self.title = title
    self.proc = proc
    self.stdout = stdout
    self.start_time = time.time()

    self.screen = pyte.Screen(80,40)
    self.stream = pyte.ByteStream()
    self.stream.attach(self.screen)
    
    self.status = "running"
    self.lock = threading.Lock()
    self.thread = threading.Thread(target=self.run)
    self.thread.start()
  
  def run(self):
    while True:
      buffer = os.read(self.stdout.fileno(), 65536)
      buffer = buffer.replace("\n", "\r\n")
      if buffer == '':
        break

      self.lock.acquire()
      self.stream.feed(buffer)
      self.lock.release()
      
    self.stdout.close()
    self.proc.wait()
    self.status = "terminated"
  
  def display(self):
    self.lock.acquire()
    b = tuple( self.screen.display )
    self.lock.release()
    return b
  
  def is_active(self):
    return True

def create_term_for_command(id, args):
  master, slave = pty.openpty()
#  attrs = termios.tcgetattr(slave)
#  print "Attrs", attrs[2]
#  attrs[2] |= termios.ONLCR
#  termios.tcsetattr(master, termios.TCSANOW, attrs)
#  p = subprocess.Popen(args, stdout = master, stderr = subprocess.STDOUT, close_fds=True)
#  return Terminal(id, p, os.fdopen(slave), " ".join(args))
  p = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, close_fds=True)
  return Terminal(id, p, p.stdout, " ".join(args))

def run_command(args):
  id = uuid.uuid4().hex
  terminals[id] = create_term_for_command(id, args)
  return flask.redirect("/terminal/"+id)

app = flask.Flask(__name__)

@app.route("/")
def index():
  instances = get_instance_counts()
  instance_table = []
  total = 0
  for instance_type, count in instances.items():
    cpus = cpus_per_instance[instance_type]
    total += cpus
    instance_table.append( (instance_type, count, cpus) )
  instance_table.append( ("Total", "", total))
  
  active_terminals = [t for t in terminals.values() if t.is_active() ]
  active_terminals.sort(lambda a,b: cmp(a.start_time, b.start_time))
  
  return flask.render_template("index.html", terminals=active_terminals, instances=instance_table)

@app.route("/terminal/<id>")
def show_terminal(id):
  if not (id in terminals):
    flask.abort(404)

  return flask.render_template("show_terminal.html", terminal=terminals[id])

@app.route("/terminal-json/<id>")
def terminal_json(id):
  if not (id in terminals):
    flask.abort(404)

  terminal=terminals[id]
  return flask.jsonify(status=terminal.status, screen=("\n".join(terminal.display())))

@app.route("/start")
def start_cluster():
  return run_command(["starcluster", "start", CLUSTER_NAME])

@app.route("/test-run")
def test_run():
  return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

@app.route("/terminate")
def terminate_cluster():
  return run_command(["starcluster", "terminate", "--confirm", CLUSTER_NAME])

def divide_into_instances(count):
  result = []

  for instance, size in instance_sizes:
    instance_count = count // size
    if instance_count == 0:
      continue
    result.append( (instance, instance_count) )
    count -= instance_count * size

  return result

@app.route("/add-nodes", methods=["POST"])
def add_nodes():
  return add_vcpus(request.values["vcpus"], request.values["price_per_vcpu"])

def add_vcpus(vcpus, price_per_vcpu):
  # only spawns the first set of machines.  Need to think if we need to support multiple
  for instance_type, count in divide_into_instances(vcpus):
    return run_command(["starcluster", "addnode", "-b", "%.4f" % (price_per_vcpu * count), "-I", instance_type, "-n", count, CLUSTER_NAME])

# ssh run job command
# starcluster add node
# starcluster rm node
# starcluster terminate
# starcluster start

# map of ID to terminal
app.run(debug=True)
    