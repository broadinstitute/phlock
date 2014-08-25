import uuid
import flask
from flask import request
import threading
import subprocess
import pyte
import pty
import functools
import formspec
import datetime
import time
import tempfile
import re

from flask.ext.openid import OpenID

import boto
import collections
import ConfigParser, os
import boto.ec2

AUTHORIZED_USERS = ['pmontgom@broadinstitute.org']

oid = OpenID(None, "/tmp/clusterui-openid")

config = ConfigParser.ConfigParser()
config.read(os.path.expanduser('~/.starcluster/config'))

if os.path.exists("/xchip/datasci/tools/venvs/starcluster/bin/starcluster"):
    STARCLUSTER_CMD = "/xchip/datasci/tools/venvs/starcluster/bin/starcluster"
    PYTHON_EXE = "/xchip/datasci/tools/venvs/clusterui/bin/python"
    DEBUG=False
else:
    STARCLUSTER_CMD = "starcluster"
    PYTHON_EXE = "python"
    DEBUG=True


AWS_ACCESS_KEY_ID = config.get("aws info", "AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws info", "AWS_SECRET_ACCESS_KEY")

def filter_inactive_instances(instances):
    return [i for i in instances if not (i.state in ['terminated', 'stopped'])]

def find_instances_in_cluster(ec2, cluster_name):
    instances = ec2.get_only_instances()
    instances = filter_inactive_instances(instances)
    group_name = "@sc-" + cluster_name
    return [i for i in instances if group_name in [g.name for g in i.groups]]


def find_master(ec2, cluster_name):
    instances = find_instances_in_cluster(ec2, cluster_name)
    matches = [i for i in instances if "Name" in i.tags and i.tags["Name"] == "master"]
    if len(matches) == 0:
        return None
    if len(matches) == 1:
        return matches[0]
    raise Exception("Too many instances named master: %s" % (matches,))


terminals = {}

instance_sizes = [("c3.large", 2), ("c3.xlarge", 4), ("c3.2xlarge", 8), ("c3.4xlarge", 16), ("c3.8xlarge", 32),
    ("r3.large", 2), ("r3.xlarge", 4), ("r3.2xlarge",8), ("r3.4xlarge",16), ("r3.8xlarge", 32)]
instance_sizes.sort(lambda a, b: -cmp(a[1], b[1]))
cpus_per_instance = {}
for instance_type, cpus in instance_sizes:
    cpus_per_instance[instance_type] = cpus
cpus_per_instance['m3.medium'] = 1

CLUSTER_NAME = "c"

ec2 = boto.ec2.connection.EC2Connection(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def get_instance_counts():
    instances = ec2.get_only_instances()
    instances = filter_inactive_instances(instances)
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

        self.screen = pyte.Screen(145, 140)
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
        b = tuple(self.screen.display)
        self.lock.release()
        return b

    def is_active(self):
        return True

    def kill(self):
        self.proc.terminate()
        for i in xrange(50):
            if self.proc.poll() != None:
                break
            time.sleep(0.1)
        if self.proc.poll() == None:
            self.proc.kill()


def create_term_for_command(id, args):
    master, slave = pty.openpty()
    # attrs = termios.tcgetattr(slave)
    # print "Attrs", attrs[2]
    # attrs[2] |= termios.ONLCR
    #  termios.tcsetattr(master, termios.TCSANOW, attrs)
    #  p = subprocess.Popen(args, stdout = master, stderr = subprocess.STDOUT, close_fds=True)
    #  return Terminal(id, p, os.fdopen(slave), " ".join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    return Terminal(id, p, p.stdout, " ".join(args))


def run_command(args):
    id = uuid.uuid4().hex
    terminals[id] = create_term_for_command(id, args)
    return flask.redirect("/terminal/" + id)


app = flask.Flask(__name__)


def redirect_with_success(msg, url):
    flask.flash(msg, 'success')
    return flask.redirect(url)


def redirect_with_error(url, msg):
    flask.flash(msg, 'danger')
    return flask.redirect(url)


def secured(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if 'email' in flask.session:
            if not (flask.session['email'] in AUTHORIZED_USERS):
                return redirect_with_error("/login", "You are not authorized to use this application")

            return fn(*args, **kwargs)
        else:
            return flask.redirect("/login")

    return wrapper

def create_or_login(resp):
    """This is called when login with OpenID succeeded and it's not
    necessary to figure out if this is the users's first login or not.
    This function has to redirect otherwise the user will be presented
    with a terrible URL which we certainly don't want.
    """
    flask.session['openid'] = resp.identity_url
    flask.session['email'] = resp.email
    return redirect_with_success("successfully signed in", oid.get_next_url())


@app.route('/login', methods=['GET', 'POST'])
@oid.loginhandler
def login():
    """Does the login via OpenID.  Has to call into `oid.try_login`
    to start the OpenID machinery.
    """
    # if we are already logged in, go back to were we came from
    if 'openid' in flask.session:
        return flask.redirect(oid.get_next_url())

    openid = "https://crowd.broadinstitute.org:8443/openidserver/op"
    return oid.try_login(openid, ask_for=['email', 'fullname'])

@app.route("/logout")
def logout():
    flask.session.pop("openid", None)
    return redirect_with_success("You were logged out", "/")


@app.route("/")
def index():
    instances = get_instance_counts()
    instance_table = []
    total = 0
    for instance_type, count in instances.items():
        cpus = cpus_per_instance[instance_type] * count
        total += cpus
        instance_table.append((instance_type, count, cpus))
    instance_table.append(("Total", "", total))

    active_terminals = [t for t in terminals.values() if t.is_active()]
    active_terminals.sort(lambda a, b: cmp(a.start_time, b.start_time))

    return flask.render_template("index.html", terminals=active_terminals, instances=instance_table)

@app.route("/add-node-form")
@secured
def add_node_form():
    instance_types = [instance_type for instance_type, size in instance_sizes]
    return flask.render_template("add-node-form.html",
                                 instance_types=instance_types)

@app.route("/add-nodes", methods=["POST"])
@secured
def add_nodes():
    return add_vcpus(int(request.values["vcpus"]), float(request.values["price_per_vcpu"]),
                     request.values["instance_type"])

@app.route("/kill-terminal/<id>")
@secured
def kill_termina(id):
    if not (id in terminals):
        flask.abort(404)

    terminals[id].kill()

    return flask.redirect("/terminal/" + id)

@app.route("/terminal/<id>")
@secured
def show_terminal(id):
    if not (id in terminals):
        flask.abort(404)

    return flask.render_template("show_terminal.html", terminal=terminals[id])


@app.route("/terminal-json/<id>")
def terminal_json(id):
    if not (id in terminals):
        flask.abort(404)

    terminal = terminals[id]
    # trim whitespace on the right
    lines = [l.rstrip() for l in terminal.display()]

    # trim empty lines from the end
    while len(lines) >= 1 and lines[-1] == '':
        del lines[-1]

    return flask.jsonify(status=terminal.status, is_running=terminal.is_running, screen=("\n".join(lines)))


@app.route("/start-cluster")
@secured
def start_cluster():
    return run_command([STARCLUSTER_CMD, "start", CLUSTER_NAME])


@app.route("/start-load-balance")
@secured
def start_load_balance():
    return run_command([STARCLUSTER_CMD, "loadbalance", CLUSTER_NAME, "-K"])


@app.route("/submit-job-form")
@secured
def submit_job_form():
    return flask.render_template("submit-job-form.html", form=formspec.ATLANTIS_FORM)


def parse_reponse(form, request_params):
    packed = {}

    by_name = collections.defaultdict(lambda: [])
    for k in request_params.keys():
        for v in request_params.getlist(k):
            by_name[k].append(v.strip())

    for field in form:
        if field.is_text:
            packed[field.label] = by_name[field.label][0]
        elif field.is_enum:
            t = by_name[field.label]
            if not field.allow_multiple:
                t = t[0]
            packed[field.label] = t

    return packed


def get_master_info():
    master = find_master(ec2, CLUSTER_NAME)

    key_location = config.get("key %s" % master.key_name, "KEY_LOCATION")
    key_location = os.path.expanduser(key_location)
    return master, key_location

TARGET_ROOT = "/data2/runs"

@app.route("/list-jobs")
def list_jobs():
    p = subprocess.Popen([STARCLUSTER_CMD, "sshmaster", CLUSTER_NAME, TARGET_ROOT+"/get_runs.py "+TARGET_ROOT], stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    stdout, stderr = p.communicate()
    jobs = json.loads(stdout)
    return flask.render_template("list-jobs.html", jobs=jobs, column_names=["status", "celllineSubset", "targetDataset", "predictiveFeatureSubset", "targetDataType", "predictiveFeatures"])

job_pattern = re.compile("\\d+-\\d+")

@app.route("/pull-job")
@secured
def pull_job():
    job_name = request.values["job"]
    assert job_pattern.match(job_name) != None
    t = tempfile.NamedTemporaryFile(delete=False)
    t.write("set +ex\n")
    t.write("cd /xchip/datasci/ec2-runs\n")
    t.write("%s sshmaster %s --user ubuntu /xchip/scripts/make_model_summaries.R /data2/runs/%s/files/results\n" % (STARCLUSTER_CMD, CLUSTER_NAME, job_name) )
    t.write("%s sshmaster %s --user ubuntu python /xchip/scripts/bundle_run_dirs.py /data2/runs/%s | tar xvzf -\n" % (STARCLUSTER_CMD, CLUSTER_NAME, job_name) )
    t.close()
    return run_command(["bash", t.name])

@app.route("/retry-job")
@secured
def retry_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_command([STARCLUSTER_CMD, "sshmaster", CLUSTER_NAME, "--user", "ubuntu", "bash "+TARGET_ROOT+"/"+job_name+"/flock-wrapper.sh retry"])

@app.route("/check-job")
@secured
def check_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_command([STARCLUSTER_CMD, "sshmaster", CLUSTER_NAME, "--user", "ubuntu", "bash "+TARGET_ROOT+"/"+job_name+"/flock-wrapper.sh check"])

@app.route("/poll-job")
@secured
def poll_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_command([STARCLUSTER_CMD, "sshmaster", CLUSTER_NAME, "--user", "ubuntu", "bash "+TARGET_ROOT+"/"+job_name+"/flock-wrapper.sh poll"])
import json

@app.route("/syncruns")
@secured
def syncRuns():
    master, key_location = get_master_info()
    return run_command([PYTHON_EXE, "-u", "syncRuns.py", master.dns_name, key_location)

@app.route("/submit-job", methods=["POST"])
@secured
def submit_job():
    params = parse_reponse(formspec.ATLANTIS_FORM, request.values)
    flock_config = formspec.apply_parameters(params)

    if "download" in request.values:
        response = flask.make_response(flock_config)
        response.headers["Content-Disposition"] = "attachment; filename=config.flock"
        return response
    else:
        master, key_location = get_master_info()

        t = tempfile.NamedTemporaryFile(delete=False)
        t.write(flock_config)
        t.close()

        t2 = tempfile.NamedTemporaryFile(delete=False)
        t2.write(json.dumps(params))
        t2.close()

        return run_command([PYTHON_EXE, "-u", "remoteExec.py", master.dns_name, key_location, params["repo"], params["branch"], t.name, TARGET_ROOT, t2.name])

@app.route("/start-tunnel")
@secured
def start_tunnel():
    master, key_location = get_master_info()

    return run_command(["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
                        "-i", key_location, "-R 8999:datasci-dev.broadinstitute.org:8999", "-N",
                        "ubuntu@" + master.dns_name])


@app.route("/test-run")
@secured
def test_run():
    return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

def isotodatetime(t):
    if t.endswith("Z"):
        t = t[:-1]
    return datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")


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
        data=[dict(x=(isotodatetime(p.timestamp) - end_time).seconds / (60.0 * 60), y=p.price / scale) for p in prices],
        name="%s %s" % (zone, instance_type),
        color="lightblue")

    result["data"].sort(lambda a, b: cmp(a["x"], b["x"]))

    return result


def normalize_series(series):
    def bucket(v):
        return int(v / 0.05) * 0.05

    all_x_values = set()
    for s in series:
        all_x_values.update([bucket(x["x"]) for x in s['data']])
    all_x_values = list(all_x_values)
    all_x_values.sort()

    def normalize(values):
        new_values = []

        xi = 0
        y = None
        for i in xrange(len(values)):
            v = values[i]
            x = bucket(v["x"])
            y = v["y"]

            while xi < len(all_x_values) and all_x_values[xi] < x:
                new_values.append(dict(x=all_x_values[xi], y=y))
                xi += 1
            new_values.append(dict(x=x, y=y))
        while xi < len(all_x_values) and y != None:
            new_values.append(dict(x=all_x_values[xi], y=y))
            xi += 1

        return new_values

    for s in series:
        s["data"] = normalize(s["data"])



def median(values):
  values = list(values)
  values.sort()
  return values[len(values)/2]

@app.route("/prices")
def prices():
    series = []
    for zone in ["us-east-1a", "us-east-1b", "us-east-1c"]:
        for t, s in instance_sizes:
            series.append(get_price_series(t, s, zone))

    normalize_series(series)
    medians = {}
    for s in series:
        medians[s["name"]] = median([x["y"] for x in s["data"]])
    medians = medians.items()
    medians.sort()

    for i in range(len(series)):
        series[i]["color"] = colors[i % len(colors)]
    return flask.render_template("prices.html", series=series, medians=medians)


@secured
@app.route("/terminate")
def terminate_cluster():
    return run_command([STARCLUSTER_CMD, "terminate", "--confirm", CLUSTER_NAME])


def divide_into_instances(count, instance_type):
    result = []

    if instance_type != "":
        return [(instance_type, count // cpus_per_instance[instance_type])]

    for instance, size in instance_sizes:
        if(not instance.startswith("c")):
            continue

        instance_count = count // size
        if instance_count == 0:
            continue
        result.append((instance, instance_count))
        count -= instance_count * size

    return result

def add_vcpus(vcpus, price_per_vcpu, instance_type):
    # only spawns the first set of machines.  Need to think if we need to support multiple
    #print "vcpus=%s, price_per_vcpu=%s, instance_type=%s" % (vcpus, price_per_vcpu, instance_type)
    for instance_type, count in divide_into_instances(vcpus, instance_type):
        command = [STARCLUSTER_CMD, "addnode", "-b", "%.4f" % (price_per_vcpu * cpus_per_instance[instance_type]), "-I",
             instance_type]
        command.extend(["-n", str(count), CLUSTER_NAME])
        return run_command( command )

import traceback
from werkzeug.debug import tbtools

@app.errorhandler(500)
def internal_error(exception):
        app.logger.exception(exception)
        tb = tbtools.get_current_traceback()
        print "traceback", tb
        return flask.render_template('500.html', exception=exception, traceback=tb.plaintext)

# map of ID to terminal
app.secret_key = 'not really secret'
oid.init_app(app)
oid.after_login_func = create_or_login
app.run(host="0.0.0.0", port=9935, debug=DEBUG)

if app.debug is not True:   
    import logging
    from logging.handlers import RotatingFileHandler
#    file_handler = RotatingFileHandler('/xchip/datasci/logs/clusterui.log', maxBytes=1024 * 1024 * 100, backupCount=20)
    file_handler = RotatingFileHandler('clusterui.log', maxBytes=1024 * 1024 * 100, backupCount=20)
    file_handler.setLevel(logging.WARN)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
