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

def find_instances_in_cluster(ec2, cluster_name):
    instances = ec2.get_only_instances()
    group_name = "@sc-"+cluster_name
    return [ i for i in instances if group_name in [g.name for g in i.groups] ] 

def find_master(ec2, cluster_name):
    instances = find_instances_in_cluster(ec2, cluster_name)
    matches = [i for i in instances if "Name" in i.tags and i.tags["Name"] == "master"]
    if len(matches) == 0:
        return None
    if len(matches) == 1:
        return matches[0]
    raise Exception("Too many instances named master: %s" % (matches,))

terminals = {}

instance_sizes = [ ("c3.large", 2), ("c3.xlarge", 4), ("c3.2xlarge", 8), ("c3.4xlarge", 16), ("c3.8xlarge", 32) ]
instance_sizes.sort(lambda a, b: -cmp(a[1], b[1]))
cpus_per_instance = {}
for instance_type, cpus in instance_sizes:
  cpus_per_instance[instance_type] = cpus
cpus_per_instance['m3.medium'] = 1

CLUSTER_NAME="c"

ec2 = boto.ec2.connection.EC2Connection(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def get_instance_counts():
  instances = ec2.get_only_instances()
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
    self.is_running = True
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
    self.is_running = False
  
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

@app.route("/add-node-form")
def add_node_form():
  return flask.render_template("add-node-form.html")
  
@app.route("/add-nodes", methods=["POST"])
def add_nodes():
  return add_vcpus(int(request.values["vcpus"]), float(request.values["price_per_vcpu"]))


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
  return flask.jsonify(status=terminal.status, is_running=terminal.is_running, screen=("\n".join(terminal.display())))

@app.route("/start-cluster")
def start_cluster():
  return run_command(["starcluster", "start", CLUSTER_NAME])

@app.route("/start-load-balance")
def start_load_balance():
  return run_command(["starcluster", "loadbalance", CLUSTER_NAME, "-K"])

import formspec
@app.route("/submit-job-form")
def submit_job_form():
  return flask.render_template("submit-job-form.html", form=formspec.ATLANTIS_FORM)

def parse_reponse(form, request_params):
  packed = {}
  
  by_name = collections.defaultdict(lambda:[])
  for k, v in request_params.items():
    by_name[k].append(v)
  
  for field in form:
    if field.is_text:
      packed[field.label] = by_name[field.label][0]
    elif field.is_enum:
      t = by_name[field.label]
      if not field.allow_multiple:
        t = t[0]
      packed[field.label] = t
  
  return packed
      
@app.route("/submit-job", methods=["POST"])
def submit_job():
  params = parse_reponse(formspec.ATLANTIS_FORM, request.values)
  flask.flash("submitting %r" % (params,))
  return flask.redirect("/")

@app.route("/start-tunnel")
def start_tunnel():
  master = find_master(ec2, CLUSTER_NAME)
  
  key_location = config.get("key %s" % master.key_name, "KEY_LOCATION")
  key_location = os.path.expanduser(key_location)
  
  return run_command(["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", 
      "-i", key_location, "-R 8999:datasci-dev.broadinstitute.org:8999", "-N", "ubuntu@"+master.dns_name])

@app.route("/test-run")
def test_run():
  return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

import datetime

def isotodatetime(t):
    if t.endswith("Z"):
        t=t[:-1]
    return datetime.datetime.strptime( t, "%Y-%m-%dT%H:%M:%S.%f" )

colors = ["#7fc97f",
    "#beaed4",
    "#fdc086",
    "#ffff99",
    "#386cb0",
    "#f0027f",
    "#bf5b17",
    "#666666"]

def get_price_series(instance_type, scale, zone="us-east-1a"):
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(days=1)
    
    prices = ec2.get_spot_price_history(start_time=start_time.isoformat(), end_time=end_time.isoformat(), 
                             instance_type=instance_type, product_description="Linux/UNIX", 
                             availability_zone=zone, max_results=None, next_token=None)
    
    result = dict(
                data=[ dict(x=(isotodatetime(p.timestamp) - end_time).seconds/(60.0*60), y=p.price/scale) for p in prices ],
                name="%s %s" % (zone, instance_type),
                color="lightblue")
                
    result["data"].sort(lambda a, b: cmp(a["x"], b["x"]))
    
    return result

def normalize_series(series):
  def bucket(v):
    return int(v / 0.05)*0.05
  all_x_values = set()
  for s in series:
    all_x_values.update([bucket(x["x"]) for x in s['data']])
  all_x_values = list(all_x_values)
  all_x_values.sort()
  def normalize(values):
    new_values = []
    
    xi = 0
    for i in xrange(len(values)):
      v = values[i]
      x = bucket(v["x"])
      y = v["y"]
      
      while xi < len(all_x_values) and all_x_values[xi] < x:
        new_values.append(dict(x=all_x_values[xi], y=y))
        xi+=1
      new_values.append(dict(x=x, y=y))
    return new_values
      
  for s in series:
    s["data"] = normalize(s["data"])

@app.route("/prices")
def prices():
  
  series = []
  for zone in ["us-east-1a", "us-east-1b", "us-east-1c"]:
    for t,s in instance_sizes:
      series.append(get_price_series(t,s,zone))
  
  normalize_series(series)
  
  for i in range(len(series)):
    series[i]["color"] = colors[i%len(colors)]
  return flask.render_template("prices.html", series=series)

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

def add_vcpus(vcpus, price_per_vcpu):
  # only spawns the first set of machines.  Need to think if we need to support multiple
  print "vcpus=%s, price_per_vcpu=%s" % (vcpus, price_per_vcpu)
  for instance_type, count in divide_into_instances(vcpus):
    return run_command(["starcluster", "addnode", "-b", "%.4f" % (price_per_vcpu * cpus_per_instance[instance_type]), "-I", instance_type, "-n", str(count), CLUSTER_NAME])

# ssh run job command
# starcluster add node
# starcluster rm node
# starcluster terminate
# starcluster start

# map of ID to terminal
app.secret_key = 'not really secret'
app.run(debug=True)
    