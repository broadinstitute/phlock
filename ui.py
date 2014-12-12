import datetime
import flask
from flask import request
import subprocess
import functools
import tempfile
import re
import term
import socket

from flask.ext.openid import OpenID

import boto
import collections
import ConfigParser, os
import boto.ec2
import boto.sdb

from werkzeug.debug import tbtools

import cluster_monitor
import argparse
import logging
import prices
from instance_types import cpus_per_instance, instance_sizes
import batch_submit
import json
import base64

oid = OpenID(None, "/tmp/clusterui-openid")
terminal_manager = term.TerminalManager()

from werkzeug.local import LocalProxy
def _get_current_config():
    return flask.current_app.config

TARGET_ROOT = "/data2/runs"

config = LocalProxy(_get_current_config)

def load_starcluster_config(app_config):
    config = ConfigParser.ConfigParser()
    config.read(app_config['STARCLUSTER_CONFIG'])
    include_paths = config.get("global", "include")
    if include_paths != None:
        include_paths = [os.path.expanduser(x) for x in include_paths.split(" ")]
        for path in include_paths:
          config.read(path)
    
    app_config['AWS_ACCESS_KEY_ID'] = config.get("aws info", "AWS_ACCESS_KEY_ID")
    app_config['AWS_SECRET_ACCESS_KEY'] = config.get("aws info", "AWS_SECRET_ACCESS_KEY")
    if not ("CLUSTER_TEMPLATE" in app_config):
        app_config["CLUSTER_TEMPLATE"] = config.get("global", "DEFAULT_TEMPLATE")

    key_name = config.get("cluster %s" % app_config["CLUSTER_TEMPLATE"], "KEYNAME")
    app_config['KEY_LOCATION'] = os.path.expanduser(config.get("key %s" % key_name, "KEY_LOCATION"))

def get_ec2_connection():
    return boto.ec2.connection.EC2Connection(aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                                        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'])

def get_sdbc_connection():
    return boto.sdb.connect_to_region("us-east-1", aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                                        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'])


def filter_stopped_instances(instances):
    return [i for i in instances if not (i.state in ['terminated', 'stopped'])]


def find_terminated_in_cluster(ec2):
    return ec2.get_only_instances(filters={"instance-state-name":"terminated"})

def find_instances_in_cluster(ec2, cluster_name):
    instances = ec2.get_only_instances()
    instances = filter_stopped_instances(instances)
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

def get_spot_prices(ec2):
    hourly_rate = 0.0
    requests = ec2.get_all_spot_instance_requests(filters={"state":"active"})
    for spot_request in requests:
        if spot_request.state == "active":
            print "spot code ", spot_request.id, spot_request.status.code
            hourly_rate += spot_request.price
    return hourly_rate

def get_instance_counts(ec2):
    instances = ec2.get_only_instances()
    instances = filter_stopped_instances(instances)

    counts = collections.defaultdict(lambda: 0)
    for i in instances:
        counts[i.instance_type] += 1

    return counts


def run_command(args, title=None):
    terminal = terminal_manager.start_term(args, title=title)
    return flask.redirect("/terminal/" + terminal.id)

def run_starcluster_cmd(args):
    return run_command([config['STARCLUSTER_CMD'], "-c", config['STARCLUSTER_CONFIG']] + args)

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
            if not (flask.session['email'] in config['AUTHORIZED_USERS']):
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

def get_cluster_state(ec2):
    try:
        groups = ec2.get_all_security_groups(groupnames=["@sc-%s" % config['CLUSTER_NAME']])
    except boto.exception.EC2ResponseError:
        return "stopped"
    assert len(groups) > 0
    return "running"

@app.route("/")
def index():
    ec2 = get_ec2_connection()
    hourly_rate = get_spot_prices(ec2)
    instances = get_instance_counts(ec2)
    instance_table = []
    total = 0
    for instance_type, count in instances.items():
        cpus = cpus_per_instance[instance_type] * count
        total += cpus
        instance_table.append((instance_type, count, cpus))
    instance_table.append(("Total", "", total))

    active_terminals = terminal_manager.get_active_terminals()
    active_terminals.sort(lambda a, b: cmp(a.start_time, b.start_time))

    cluster_state = get_cluster_state(ec2)
    manager_state = cluster_manager.get_manager_state()

    request_counts = collections.defaultdict(lambda: 0)
    for request in ec2.get_all_spot_instance_requests(filters={"state":"open"}):
        name = (request.launch_specification.instance_type, request.price, request.status.message)
        request_counts[name] += 1

    open_spot_requests = []
    for k in request_counts.keys():
        instance_type, price, status = k
        count = request_counts[k]
        cpus = cpus_per_instance[instance_type] * count
        open_spot_requests.append( (instance_type, status, price, count, cpus) )

    return flask.render_template("index.html", terminals=active_terminals, instances=instance_table, cluster_state=cluster_state,
                                 manager_state=manager_state,
                                 hourly_rate=hourly_rate, open_spot_requests=open_spot_requests)

@app.route("/start-manager")
@secured
def start_manager():
    cluster_manager.start_manager()
    return redirect_with_success("Manager started", "/")

@app.route("/stop-manager")
@secured
def stop_manager():
    cluster_manager.stop_manager()
    return redirect_with_success("Manager stopped", "/")


@app.route("/kill-terminal/<id>")
@secured
def kill_terminal(id):
    terminal = terminal_manager.get_terminal(id)
    if terminal == None:
        flask.abort(404)

    terminal.kill()

    return flask.redirect("/terminal/" + id)


@app.route("/terminal/<id>")
@secured
def show_terminal(id):
    terminal = terminal_manager.get_terminal(id)
    if terminal == None:
        flask.abort(404)

    return flask.render_template("show_terminal.html", terminal=terminal)


@app.route("/terminal-json/<id>")
def terminal_json(id):
    terminal = terminal_manager.get_terminal(id)
    if terminal == None:
        flask.abort(404)

    # trim whitespace on the right
    lines = [l.rstrip() for l in terminal.display()]

    # trim empty lines from the end
    while len(lines) >= 1 and lines[-1] == '':
        del lines[-1]

    return flask.jsonify(status=terminal.status, is_running=terminal.is_running, screen=("\n".join(lines)))

@app.route("/start-cluster")
@secured
def start_cluster():
    cluster_manager.start_cluster()
    return redirect_to_cluster_terminal()

@secured
@app.route("/stop-cluster")
def stop_cluster():
    cluster_manager.stop_cluster()
    return redirect_to_cluster_terminal()


def parse_reponse(fields, request_params, files):
    packed = {}

    by_name = collections.defaultdict(lambda: [])
    for k in request_params.keys():
        for v in request_params.getlist(k):
            by_name[k].append(v.strip())

    for field in fields:
        if field.is_text:
            packed[field.label] = by_name[field.label][0]
        elif field.is_enum:
            t = by_name[field.label]
            if not field.allow_multiple:
                t = t[0]
            packed[field.label] = t
        elif field.is_file:
            file = files[field.label]
            packed[field.label] = file.read()
        else:
            raise Exception("unknown field type")

    return packed


def get_master_info(ec2):
    master = find_master(ec2, config['CLUSTER_NAME'])

    return master, config['KEY_LOCATION']

import adhoc

CONFIGS = { "bulk": [ {"name":["bulk"], "targetDataset": ["ach2.12"], "targetDataType": ["gene solutions","GS t/h lv2","GS t/h lv3"], "celllineSubset": ["all","solid"],
  "predictiveFeatureSubset": ["top100", "all", "single", "bydomain", "byseqparalog", "physicalinteractors"],
  "predictiveFeatures": [ ["GE"], ["CN"], ["MUT"], ["GE", "CN", "MUT"] ] } ] }

import sshxmlrpc
import threading
per_thread_cache = threading.local()
from werkzeug.local import LocalProxy

def get_wingman_service_factory():
    app = flask.current_app
    if isinstance(app, LocalProxy):
        print "getting app from proxy"
        app = app._get_current_object()
    else:
        print "app is not a proxy"

    def factory():
        with app.app_context():
            ec2 = get_ec2_connection()
            master, key_location = get_master_info(ec2)
            master_dns_name = master.dns_name

        # perhaps we should ask the wingman service for the names of all methods?  That would avoid hardcoding them here
        client_methods = set(["get_run_files", "get_file_content", "delete_run", "retry_run", "kill_run",
                              "run_created", "run_submitted", "taskset_created", "task_submitted", "task_started",
                              "task_failed", "task_completed", "node_disappeared", "get_version", "get_runs",
                              "set_required_mem_override", "get_run_tasks"])
        return sshxmlrpc.SshXmlServiceProxy(master_dns_name, "ubuntu", key_location, 3010, client_methods)

    return factory

def get_wingman_service():
    if hasattr(per_thread_cache, "wingman_service_factory"):
        wingman_service_factory = per_thread_cache.wingman_service_factory
    else:
        wingman_service_factory = get_wingman_service_factory()
        per_thread_cache.wingman_service_factory = wingman_service_factory
    return wingman_service_factory()

def get_jobs_from_remote():
    return get_wingman_service().get_runs()

@app.route("/run/<run_name>")
def show_run(run_name):
    run_dir = TARGET_ROOT + "/" + run_name + "/files"
    tasks = get_wingman_service().get_run_tasks(run_dir)
    def rewrite_task(t):
        t = dict(t)
        # drop run prefix
        t['task_dir'] = t['task_dir'][len(TARGET_ROOT)+3+len("files")+len(run_name):]
        return t
    tasks = [rewrite_task(t) for t in tasks]
    return flask.render_template("show-run.html", run_name=run_name, tasks=tasks)


@app.route("/list-run-files/<run_name>", defaults=dict(file_path=""))
@app.route("/list-run-files/<run_name>/<path:file_path>")
def list_run_files(run_name, file_path):

    run_dir = TARGET_ROOT + "/" + run_name + "/files"

    if file_path == "":
        wildcard = "*"
    else:
        wildcard = file_path+"/*"

    files = get_wingman_service().get_run_files(run_dir, wildcard)
    for file in files:
        file["name"] = os.path.basename(file["name"])
        if file_path == "":
            file["rel_path"] = file["name"]
        else:
            file["rel_path"] = file_path + "/" + file["name"]
    files.sort(lambda a, b: cmp(a["name"], b["name"]))
    return flask.render_template("list-run-files.html", files=files, file_path=file_path, run_name=run_name)

@app.route("/view-run-file/<run_name>/<path:file_path>")
def view_run_file(run_name, file_path):
    run_dir = TARGET_ROOT + "/" + run_name + "/files"

    service = get_wingman_service()
    files = service.get_run_files(run_dir, file_path)
    assert len(files) == 1
    length = files[0]['size']
    def stream_file():
        offset = 0
        read_size = 50000
        while offset < length:
            payload = service.get_file_content(run_dir, file_path, offset, read_size)
            content = base64.standard_b64decode(payload['data'])
            offset += read_size
            if offset < length:
                assert len(content) == read_size
            yield content

    # hack: is there some place we can ask something more complete for a mimetype given an extension?
    mimetype="application/octet-stream"
    if file_path.endswith(".pdf"):
        mimetype="application/pdf"
    elif file_path.endswith(".txt"):
        mimetype="text/plain"

    return flask.Response(stream_file(), mimetype=mimetype)

@app.route("/job-dashboard")
def job_dashboard():
    filter_tags = request.values.getlist("tag")
    config_name = request.values.get("config_name")

    existing_jobs = get_jobs_from_remote()

    if config_name == "" or config_name == None:
        config = None
        config_name = ""
    else:
        config = CONFIGS[config_name]

    if config != None:
        # compute set of all columns used to define a unique job
        config_property_names = set()
        for x in config:
            config_property_names.update(x.keys())

        all_configs = adhoc.enumerate_configurations(config)
        existing_jobs_parameters = [x['parameters'] for x in existing_jobs]
        existing_jobs_status = dict([ (x['parameters']['run_id'], x['status'])])
        merged = adhoc.find_each_in_a(config_property_names, all_configs, existing_jobs_parameters)
        flattened = adhoc.flatten(merged, existing_jobs_status)
    else:
        flattened = adhoc.from_existing(existing_jobs)

    flattened = adhoc.find_with_tags(flattened, filter_tags)

    summary = adhoc.summarize(flattened)
    for x in ["run_id", "job_dir", "status", "job_name"]:
        if x in summary:
            del summary[x]

    fixed_values, column_names = adhoc.factor_out_common(summary)
    tag_universe = adhoc.get_all_tags(summary)

    return flask.render_template("job-dashboard.html", jobs=flattened,
                                 column_names=column_names, tags=tag_universe,
                                 fixed_values=fixed_values.items(), current_filter_tags=filter_tags,
                                 config_name=config_name, config_names=CONFIGS.keys())

job_pattern = re.compile("\\d+-\\d+")

def parse_and_validate_jobs(job_ids_json):
    job_ids = json.loads(job_ids_json)
    for job_id in job_ids:
        assert job_pattern.match(job_id) != None
    return job_ids


def get_run_dir_for_job_name(job_name):
    return "%s/%s/files" % (TARGET_ROOT, job_name)

@app.route("/archive-jobs", methods=["POST"])
@secured
def archive_jobs():
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    destination = request.values["destination"]

    assert re.match('^[\w-]+$', destination) is not None

    service = get_wingman_service()
    for job_name in job_ids:
        service.delete_run(get_run_dir_for_job_name(job_name))

    return run_starcluster_cmd(["sshmaster", config['CLUSTER_NAME'], "--user", "ubuntu",
                            "mv " + " ".join([TARGET_ROOT + "/" + job_id for job_id in job_ids])+ " " + os.path.join(TARGET_ROOT, destination)])

@app.route("/retry-jobs", methods=["POST"])
@secured
def retry_job():
    service = get_wingman_service()
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    for job_name in job_ids:
        service.retry_run(get_run_dir_for_job_name(job_name))
    return redirect_with_success("retried %d jobs" % len(job_ids), "/")


@app.route("/kill-jobs", methods=["POST"])
@secured
def kill_job():
    service = get_wingman_service()
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    for job_name in job_ids:
        service.kill_run(get_run_dir_for_job_name(job_name))
    return redirect_with_success("killed %d jobs" % len(job_ids), "/")

@app.route("/job-set-mem-override", methods=["POST"])
@secured
def job_set_mem_override():
    service = get_wingman_service()
    mem_limit = int(request.values["mem-limit"])
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    for job_name in job_ids:
        service.set_required_mem_override(get_run_dir_for_job_name(job_name), mem_limit)
    return redirect_with_success("changed mem limit on %d jobs" % len(job_ids), "/")


def get_current_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def submit_job(flock_config, params, nowait):
    assert params["repo"] != None
    assert params["branch"] != None
    assert params["run_id"] != None
    assert params["sha"] != None

    ec2 = get_ec2_connection()
    master, key_location = get_master_info(ec2)

    t = tempfile.NamedTemporaryFile(delete=False)
    t.write(flock_config)
    t.close()

    t2 = tempfile.NamedTemporaryFile(delete=False)
    t2.write(json.dumps(params))
    t2.close()

    sorted_keys = params.keys()
    sorted_keys.sort()

    run_id = params['run_id']
    title = "Run %s: %s" % (run_id, ", ".join([str(params[k]) for k in sorted_keys if not (k in ["run_id", "sha", "repo", "branch", "config"]) ]))

    return run_command(
        [config['PYTHON_EXE'], "-u", "remoteExec.py", master.dns_name, key_location, params["repo"], params["branch"], t.name,
         TARGET_ROOT, t2.name, run_id, config["FLOCK_PATH"], params['sha'], "nowait" if nowait else "wait"], title=title)


def get_sha(repo, branch):
    proc = subprocess.Popen(["git", "ls-remote", repo], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    pairs = [line.split("\t") for line in stdout.split("\n")]
    for sha, tag in pairs:
        if tag == branch:
            return sha

    raise Exception("Could not find %s in %s, stderr=%s" % (branch, repr(pairs), repr(stderr)))


@app.route("/submit-batch-job-form")
@secured
def submit_batch_job_form():
    return flask.render_template("submit-batch-job-form.html", default_repo="ssh://git@stash.broadinstitute.org:7999/cpds/atlantis.git", default_branch="refs/heads/master")

@app.route("/submit-batch-job", methods=["POST"])
@secured
def submit_batch_job():
    template_file = request.files["template"]
    template_str = template_file.read()

    if request.values['config_defs'] == "":
        config_defs = [{}]
    else:
        config_defs = json.loads(request.values['config_defs'])

    timestamp = get_current_timestamp()
    repo = request.values["repo"]
    branch = request.values["branch"]
    sha = get_sha(repo, branch)
    json_and_flock = batch_submit.make_flock_configs(config_defs, template_str, timestamp, {"repo": repo, "branch": branch, "sha": sha})

    submit = request.values['submit']
    if submit == 'submit':
        for params, flock in json_and_flock:
            submit_job(flock, params, True)
        return redirect_with_success("submitted %d jobs" % (len(json_and_flock)), "/")

    elif submit == 'export':
        params, flock_config = json_and_flock[0]
        response = flask.make_response(flock_config)
        response.headers["Content-Disposition"] = "attachment; filename=config.flock"
        return response
    else:
        raise Exception("invalid value for submit (%s)" % submit)

monitor_parameters = cluster_monitor.Parameters()
cluster_terminal = None
cluster_manager = None

def redirect_to_cluster_terminal():
    return flask.redirect("/terminal/" + cluster_terminal.id)

@app.route("/edit-monitor-parameters")
@secured
def edit_monitor_request():
    return flask.render_template("edit_monitor_parameters.html", param=monitor_parameters)

@app.route("/set-monitor-parameters", methods=["POST"])
@secured
def set_monitor_parameters():
    values = request.values
    monitor_parameters.spot_bid=float(values['spot_bid'])
    monitor_parameters.max_to_add=int(values['max_to_add'])
    monitor_parameters.instance_type=values['instance_type']
    monitor_parameters.max_instances=int(values['max_instances'])
    monitor_parameters.min_instances=int(values['min_instances'])
    monitor_parameters.job_wait_time = int(values['job_wait_time'])
    monitor_parameters.stabilization_time = int(values['stabilization_time'])

    return flask.redirect("/")

@app.route("/start-tunnel")
@secured
def start_tunnel():
    return run_starcluster_cmd(["runplugin", "taigaTunnel", config['CLUSTER_NAME']])

@app.route("/test-run")
@secured
def test_run():
    return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

@app.route("/show-instances")
def show_instances():
    ec2 = get_ec2_connection()
    spots = ec2.get_all_spot_instance_requests(filters={"state":"open"})
    instances = ec2.get_only_instances()
    instances = filter_stopped_instances(instances)
    return flask.render_template("show-instances.html", instances=instances, spots=spots)

@app.route("/kill-instances")
@secured
def kill_instances():
    ids = request.values.getlist("id")
    ec2 = get_ec2_connection()
    ec2.terminate_instances(instance_ids=ids)
    return redirect_with_success("terminated %d instances"%len(ids), "/")


@app.route("/kill-spots")
@secured
def kill_spots():
    ids = request.values.getlist("id")
    ec2 = get_ec2_connection()
    ec2.cancel_spot_instance_requests(ids)
    return redirect_with_success("cancelled %d spot requests"%len(ids), "/")

@app.route("/prices")
def show_prices():
    ec2 = get_ec2_connection()

    series = []
    for zone in ["us-east-1a", "us-east-1b", "us-east-1c"]:
        for t, s in instance_sizes:
            series.append(prices.get_price_series(ec2, t, s, zone))

    prices.normalize_series(series)
    per_instance_price = {}
    for s in series:
        values = [x["y"] for x in s["data"]]
        per_instance_price[s["name"]] = (prices.median(values), values[-1])
    per_instance_price = [(n, v[0], v[1]) for n, v in per_instance_price.items()]
    per_instance_price.sort()

    for i in range(len(series)):
        series[i]["color"] = prices.colors[i % len(prices.colors)]
    return flask.render_template("prices.html", series=series, per_instance_price=per_instance_price)


@app.errorhandler(500)
def internal_error(exception):
    app.logger.exception(exception)
    tb = tbtools.get_current_traceback()
    print "traceback", tb
    return flask.render_template('500.html', exception=exception, traceback=tb.plaintext)


@app.before_first_request
def init_manager():
    global cluster_terminal
    global cluster_manager

    log_file = None

    if "MONITOR_LOG" in config:
        log_file = open(config["MONITOR_LOG"], "a")

    if "MONITOR_JSON_LOG" in config:
        monitor_parameters.log_file = config["MONITOR_JSON_LOG"]

    cluster_terminal = terminal_manager.start_named_terminal("Cluster monitor", log_file=log_file)
    instance_id = "host=%s, pid=%d" % (socket.getfqdn(), os.getpid())
    cluster_manager = cluster_monitor.ClusterManager(monitor_parameters,
                                                     config['CLUSTER_NAME'],
                                                     config["CLUSTER_TEMPLATE"],
                                                     cluster_terminal,
                                                     [config['STARCLUSTER_CMD'], "-c", config['STARCLUSTER_CONFIG']],
                                                     instance_id,
                                                     get_ec2_connection(),
                                                     config['LOADBALANCE_PID_FILE'],
                                                     get_sdbc_connection(),
                                                     get_wingman_service_factory())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start webserver for cluster ui')
    parser.add_argument('--config', dest="config_path", help='config file to use', default=os.path.expanduser("~/.clusterui.config"))
    args = parser.parse_args()

    app.config.update(LOG_FILE='clusterui.log',
                      DEBUG=True,
                      PORT=9935,
                      FLOCK_PATH="/xchip/flock/bin/phlock",
                      LOADBALANCE_PID_FILE="loadbalance.pid")
    app.config.from_pyfile(args.config_path)
    load_starcluster_config(app.config)

    oid.init_app(app)
    oid.after_login_func = create_or_login

    app.run(host="0.0.0.0", port=app.config['PORT'], debug=app.config['DEBUG'])

    if app.debug is not True:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(app.config['LOG_FILE'], maxBytes=1024 * 1024 * 100, backupCount=20)
        file_handler.setLevel(logging.WARN)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        app.logger.addHandler(file_handler)
