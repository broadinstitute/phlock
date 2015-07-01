import datetime
import flask
from flask import request
import subprocess
import tempfile
import re
import term
import socket
import sys
import traceback
from hashlib import md5
import shelve

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
from instance_types import cpus_per_instance
import batch_submit
import json
import base64

oid = OpenID()
terminal_manager = term.TerminalManager()
log = logging.getLogger("ui")

from werkzeug.local import LocalProxy
def _get_current_config():
    return flask.current_app.config

config = LocalProxy(_get_current_config)

def convert_to_boolean(name, value):
    value = value.lower()
    if value == 'true':
        return True
    if value == 'false':
        return False
    raise Exception("Expected either 'True' or 'False' for %s but got %s" %(name, repr(value)))

def load_starcluster_config(app_config):
    config = ConfigParser.ConfigParser()
    config.read(app_config['STARCLUSTER_CONFIG'])

    """
    if the include section exist, process it
    """
    if config.has_option("global", "include"):
        try:
            include_paths = config.get("global", "include")
            include_paths = [os.path.expanduser(x) for x in include_paths.split(" ")]
            for path in include_paths:
                config.read(path)
        except ConfigParser.NoOptionError:
            pass

    app_config['AWS_ACCESS_KEY_ID'] = config.get("aws info", "AWS_ACCESS_KEY_ID")
    app_config['AWS_SECRET_ACCESS_KEY'] = config.get("aws info", "AWS_SECRET_ACCESS_KEY")
    if not ("CLUSTER_TEMPLATE" in app_config):
        app_config["CLUSTER_TEMPLATE"] = config.get("global", "DEFAULT_TEMPLATE")

    try:
        dns_prefix = convert_to_boolean("DNS_PREFIX", config.get("cluster %s" % app_config["CLUSTER_TEMPLATE"], "DNS_PREFIX"))
    except ConfigParser.NoOptionError:
        dns_prefix = False

    if dns_prefix:
        app_config["MASTER_NODE_NAME"] = app_config["CLUSTER_NAME"] + "-master"
    else:
        app_config["MASTER_NODE_NAME"] = "master"

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
    return execute_with_retry(lambda: ec2.get_only_instances(filters={"instance-state-name":"terminated"}), [boto.exception.EC2ResponseError])

def execute_with_retry(fn, exception_types, retry_count=3):
    for i in range(retry_count):
        try:
            return fn()
        except:
            error_type, error_instance, t = sys.exc_info()
            if not (error_type in exception_types):
                raise
            else:
                traceback.print_exc()
                print("Call failed, retrying...")
    raise Exception("Too many failures.  Aborting")


def find_instances_in_cluster(ec2, cluster_name):
    instances = execute_with_retry(ec2.get_only_instances, [boto.exception.EC2ResponseError])
    instances = filter_stopped_instances(instances)
    group_name = "@sc-" + cluster_name
    return [i for i in instances if group_name in [g.name for g in i.groups]]


def find_master(ec2):
    cluster_name = config['CLUSTER_NAME']
    instances = find_instances_in_cluster(ec2, cluster_name)
    matches = [i for i in instances if "Name" in i.tags and i.tags["Name"] == config["MASTER_NODE_NAME"]]
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
    instances = execute_with_retry(ec2.get_only_instances, [boto.exception.EC2ResponseError])
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
    # made this into a noop when I made a global login requirement
    return fn

def create_or_login(resp):
    """This is called when login with OpenID succeeded and it's not
    necessary to figure out if this is the users's first login or not.
    This function has to redirect otherwise the user will be presented
    with a terrible URL which we certainly don't want.
    """
    flask.session['openid'] = resp.identity_url
    flask.session['email'] = resp.email
    return redirect_with_success("successfully signed in", oid.get_next_url())

@app.template_filter("format_disk_space")
def format_disk_space(amount):
    mb = float(1024*1024)
    gb = 1024*mb
    if amount > gb:
      amount = amount/gb
      suffix = "GB"
    elif amount > mb:
      amount = amount/mb
      suffix = "MB"
    else:
      amount = amount/1024
      suffix = "KB"
    return u'{0:.3f}{1}'.format(amount, suffix)

@app.template_filter("format_minutes")
def format_minutes(amount):
    day = 60*24
    hour = 60 
    if amount > day:
      suffix = "days"
      amount = amount/day
    elif amount > hour:
      suffix = "hours"
      amount = amount/hour
    else:
      suffix = "minutes"
    return u'{0:.3f} {1}'.format(amount, suffix)
                
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


@app.before_request
def ensure_logged_in_before_request():
    if not (request.endpoint in ['login', 'not_authorized']):
        if 'email' in flask.session:
            if not (flask.session['email'] in config['AUTHORIZED_USERS']):
                return redirect_with_error("/not_authorized", "You are not authorized to use this application")
            else:
                return None
        else:
            return flask.redirect("/login")

    return None


@app.route("/not_authorized")
def not_authorized():
    return flask.render_template("not_authorized.html")

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

    master = find_master(ec2)
    if master == None:
        master_node_address = "No master node running"
    else:
        master_node_address = master.dns_name

    return flask.render_template("index.html", terminals=active_terminals, instances=instance_table, cluster_state=cluster_state,
                                 manager_state=manager_state,
                                 hourly_rate=hourly_rate, open_spot_requests=open_spot_requests, master_node_address=master_node_address)

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
    master = find_master(ec2)

    return master, config['KEY_LOCATION']

import adhoc


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
                              "set_required_mem_override", "get_run_tasks", "get_host_summary", "archive_run", "list_archives",
                              "set_tag"])
        return sshxmlrpc.SshXmlServiceProxy(master_dns_name, "ubuntu", key_location, 3010, client_methods)

    return factory

def get_wingman_service():
    if hasattr(per_thread_cache, "wingman_service_factory"):
        wingman_service_factory = per_thread_cache.wingman_service_factory
    else:
        wingman_service_factory = get_wingman_service_factory()
        per_thread_cache.wingman_service_factory = wingman_service_factory
    return wingman_service_factory()

def get_jobs_from_remote(archive_name=None):
    return get_wingman_service().get_runs(archive_name)

def get_run_files_path(run_name):
    return config['TARGET_ROOT'] + "/" + run_name + "/files"

@app.route("/run/<run_name>")
def show_run(run_name):
    run_dir = get_run_files_path(run_name)
    tasks = get_wingman_service().get_run_tasks(run_dir)
    def rewrite_task(t):
        t = dict(t)
        # drop run prefix
        t['task_dir'] = t['task_dir'][len(config['TARGET_ROOT'])+3+len("files")+len(run_name):]
        return t
    tasks = [rewrite_task(t) for t in tasks]
    return flask.render_template("show-run.html", run_name=run_name, tasks=tasks)


@app.route("/list-run-files/<run_name>", defaults=dict(file_path=""))
@app.route("/list-run-files/<run_name>/<path:file_path>")
def list_run_files(run_name, file_path):

    if file_path == "":
        wildcard = "*"
    else:
        wildcard = file_path+"/*"

    files = get_wingman_service().get_run_files(run_name, wildcard)
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
    service = get_wingman_service()
    files = service.get_run_files(run_name, file_path)
    assert len(files) == 1
    length = files[0]['size']
    def stream_file():
        offset = 0
        read_size = 50000
        while offset < length:
            payload = service.get_file_content(run_name, file_path, offset, read_size)
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

cached_projections = None
cached_projections_mtime = None

def load_projections():
    global cached_projections, cached_projections_mtime
    if not ("PROJECTIONS_PATH" in config):
      return {}
    
    projections_path = config["PROJECTIONS_PATH"]
    mtime = os.path.getmtime(projections_path)
    
    if cached_projections_mtime != mtime:
      fd = open(projections_path)
      cached_projections = eval(fd.read())
      cached_projections_mtime = mtime
    
    return cached_projections
    

@app.route("/job-dashboard")
def job_dashboard():
    projections = load_projections()

    filter_tags = request.values.getlist("tag")
    config_name = request.values.get("config_name")
    if config_name == "":
        config_name = None
    archive_name = request.values.get("archive_name")
    if archive_name == "":
        archive_name = None

    existing_jobs = get_jobs_from_remote(archive_name)

    # overlay the tags with 'added_tags'
    for job in existing_jobs:
        added_tags = job['added_tags']
        if added_tags == None:
            added_tags = {}
        for k, v in added_tags.items():
            job["parameters"][k] = v

    archive_names = get_wingman_service().list_archives()
    archive_names.sort()

    if config_name == "" or config_name == None:
        config = None
        config_name = ""
    else:
        config = projections[config_name]

    if config != None:
        # compute set of all columns used to define a unique job
        config_property_names = set()
        for x in config:
            config_property_names.update(x.keys())

        all_configs = adhoc.enumerate_configurations(config)
        existing_jobs_parameters = [x['parameters'] for x in existing_jobs]
        existing_jobs_status = dict([ (x['parameters']['run_id'], x['status']) for x in existing_jobs])
        merged = adhoc.find_each_in_a(config_property_names, all_configs, existing_jobs_parameters)
        flattened = adhoc.flatten(merged, existing_jobs_status)
    else:
        flattened = adhoc.from_existing(existing_jobs)

    flattened = adhoc.find_with_tags(flattened, filter_tags)

    summary = adhoc.summarize(flattened)
    for x in ["run_id", "job_dir", "status", "job_name", "parameter_hash"]:
        if x in summary:
            del summary[x]

    fixed_values, column_names = adhoc.factor_out_common(summary)
    tag_universe = adhoc.get_all_tags(summary)

    return flask.render_template("job-dashboard.html", jobs=flattened,
                                 column_names=column_names, tags=tag_universe,
                                 fixed_values=fixed_values.items(), current_filter_tags=filter_tags,
                                 config_name=config_name, config_names=projections.keys(),
                                 archive_names=archive_names, archive_name=archive_name)

job_pattern = re.compile("\\d+-\\d+")

def parse_and_validate_jobs(job_ids_json):
    job_ids = json.loads(job_ids_json)
    for job_id in job_ids:
        assert job_pattern.match(job_id) != None
    return job_ids


def get_run_dir_for_job_name(job_name):
    return "%s/%s/files" % (config['TARGET_ROOT'], job_name)

@app.route("/archive-jobs", methods=["POST"])
@secured
def archive_jobs():
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    destination = request.values["destination"]

    assert re.match('^[\w._-]+$', destination) is not None

    service = get_wingman_service()
    for job_name in job_ids:
        service.archive_run(job_name, destination)

    return redirect_with_success("archived %d jobs" % len(job_ids), "/")

@app.route("/retry-jobs", methods=["POST"])
@secured
def retry_job():
    service = get_wingman_service()
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    for job_name in job_ids:
        service.retry_run(get_run_dir_for_job_name(job_name))
    return redirect_with_success("retried %d jobs" % len(job_ids), "/")


@app.route("/job-set-tag", methods=["POST"])
@secured
def set_tag():
    service = get_wingman_service()
    job_ids = parse_and_validate_jobs(request.values["job-ids"])
    tag_name = request.values['tag']
    tag_value = request.values['value']
    for job_name in job_ids:
        service.set_tag(job_name, tag_name, tag_value)
    return redirect_with_success("set %s = %s on %d jobs" % (tag_name, tag_value, len(job_ids)), "/")

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


def generate_run_command(flock_config, params):
    assert params["run_id"] != None

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

    cmd = [config['PYTHON_EXE'], "-u", "remoteExec.py", "exec-only", master.dns_name, key_location, t.name, config['TARGET_ROOT'], t2.name, run_id]
    title = "Run %s: %s" % (run_id, ", ".join([str(params[k]) for k in sorted_keys if not (k in ["run_id", "sha", "repo", "branch", "config"]) ]))

    return (cmd, title)

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
    parameters = {'repo':"ssh://git@stash.broadinstitute.org:7999/cpds/atlantis.git", 'branch':"refs/heads/master"}

    if "parameter_hash" in request.values:
        hash = str(request.values["parameter_hash"])

        m = shelve.open(config["SUBMISSION_HISTORY"])
        if hash in m:
            parameters_json = m[hash]
            parameters = json.loads(parameters_json)
        else:
            flask.flash("Could not find parameters", 'danger')


    return flask.render_template("submit-batch-job-form.html", **parameters)

@app.route("/submit-flock-job-form")
@secured
def submit_flock_job_form():
    return flask.render_template("submit-flock-job-form.html")

@app.route("/submit-flock-job", methods=["POST"])
@secured
def submit_flock_job():
    flock_config = request.files["flock_config"]
    flock_config_str = flock_config.read()

    timestamp = get_current_timestamp()

    cmd, title = generate_run_command(flock_config_str, {'run_id': timestamp})
    return run_command(cmd, title=title)

def get_code_cache_dir(sha):
    return config['TARGET_ROOT'] + "/code-cache/"+sha

def deploy_code_from_git_command(repo, branch, sha):
    ec2 = get_ec2_connection()
    master, key_location = get_master_info(ec2)

    sha_code_dir = get_code_cache_dir(sha)

    args = [config['PYTHON_EXE'], "-u", "deploy_from_git.py", master.dns_name, key_location, repo, branch, sha_code_dir]
    return args

def write_shell_command(fd, command):
    quoted = []
    for x in command:
        quoted.append("'"+x+"'")
    fd.write(" ".join(quoted)+"\n")

def save_submit_parameters(config_defs, template_str, repo, branch, sha):
    parameters_json = json.dumps(dict(config_defs=config_defs, template_str=template_str, repo=repo, branch=branch, sha=sha), sort_keys=True)
    parameter_hash = md5(parameters_json).hexdigest()
    m = shelve.open(config["SUBMISSION_HISTORY"])
    # save parameters
    m[parameter_hash] = parameters_json
    m.close()

    return parameter_hash

@app.route("/submit-batch-job", methods=["POST"])
@secured
def submit_batch_job():
    # hack assumes that username portion of email address is sufficient
    username = flask.session['email'].split("@")[0]
    if "template_file" in request.files:
        template_file = request.files["template_file"]
        template_str = template_file.read()

    if template_str == "":
        template_str = request.values["template"]

    repo = request.values["repo"]
    branch = request.values["branch"]
    sha = get_sha(repo, branch)

    if request.values['config_defs'] == "":
        config_defs = [{}]
    else:
        config_defs = json.loads(request.values['config_defs'])

    timestamp = get_current_timestamp()

    parameter_hash = save_submit_parameters(config_defs, template_str, repo, branch, sha)

    json_and_flock = batch_submit.make_flock_configs(config_defs, template_str, timestamp, {"repo": repo, "branch": branch, "sha": sha, 'username': username, "parameter_hash": parameter_hash})

    submit = request.values['submit']
    if submit == 'submit':
        # make a temp script which performs the deploy and kicks off the jobs.  Why do it like this?  Mostly legacy because
        # the pieces to do this are in separate scripts.  The code could be all run within this process but having it run from a script
        # does add some isolation in case something fails in an unexpected way.  This is all a little ridiculous and hacky but it should work.

        t = tempfile.NamedTemporaryFile(delete=False)
        t.write("set -ex\n")
        cmd = deploy_code_from_git_command(repo, branch, sha)
        write_shell_command(t, cmd)
        for params, flock in json_and_flock:
            cmd, title = generate_run_command(flock, params)
            write_shell_command(t, cmd)

        t.close()

        return run_command(["bash", t.name], "submitting %d jobs" % (len(json_and_flock)))

    elif submit == 'export':
        params, flock_config = json_and_flock[0]
        response = flask.make_response(flock_config)
        response.headers["Content-Disposition"] = "attachment; filename=config.flock"
        return response
    else:
        raise Exception("invalid value for submit (%s)" % submit)

monitor_parameters = None
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
    monitor_parameters.reserve_node_timeout = int(values["reserve_node_timeout"])
    monitor_parameters.reserve_nodes = int(values["reserve_nodes"])
    monitor_parameters.ignore_grp = "ignore_grp" in values

    print "ignore_grp", monitor_parameters.ignore_grp

    return flask.redirect("/")

@app.route("/start-tunnel")
@secured
def start_tunnel():
    return run_starcluster_cmd(["runplugin", "taigaTunnel", config['CLUSTER_NAME']])

@app.route("/test-run")
@secured
def test_run():
    return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

@app.route("/show-sge-state")
def show_sge_state():
    service = get_wingman_service()
    state = service.get_host_summary()
    return flask.render_template("show-sge-state.html", **state)

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

def read_task_profile(run_dir, file_path):
    import StringIO
    import csv

    service = get_wingman_service()
    files = service.get_run_files(run_dir, file_path)
    assert len(files) == 1
    length = files[0]['size']

    buffer = StringIO.StringIO()

    offset = 0
    read_size = 50000
    while offset < length:
        payload = service.get_file_content(run_dir, file_path, offset, read_size)
        content = base64.standard_b64decode(payload['data'])
        offset += read_size
        if offset < length:
            assert len(content) == read_size
        buffer.write(content)

    buffer.seek(0)
    reader = csv.DictReader(buffer, delimiter=" ")
    l_utime = []
    l_stime = []
    l_vsizeMB = []
    l_rssMB = []
    prev_x = None
    first_x = None
    for row in reader:
        if row["utime"] == "NA":
            continue

        x = float(row["timestamp"])
        if first_x == None:
            first_x = x
        time_in_minutes = (x-first_x) / (60.0)
        utime = float(row["utime"])
        stime = float(row["stime"])

        if prev_x != None:
            delta_x = x - prev_x
            l_utime.append(dict(x=time_in_minutes, y=100*(utime-prev_utime)/delta_x))
            l_stime.append(dict(x=time_in_minutes, y=100*(stime-prev_stime)/delta_x))

        prev_utime = utime
        prev_stime = stime
        prev_x = x

        l_vsizeMB.append(dict(x=time_in_minutes, y=float(row["vsizeMB"])))
        l_rssMB.append(dict(x=time_in_minutes, y=float(row["rssMB"])))

    return ([dict(data=l_utime, color="lightblue", name="utime"),
            dict(data=l_stime, color="black", name="stime")],
            [dict(data=l_vsizeMB, color="red", name="vsizeMB"),
             dict(data=l_rssMB, color="black", name="rssMB")])


@app.route("/show-task-profile/<run_name>/<path:task_dir>")
def show_task_profile(run_name, task_dir):
    # result = dict(
    #     data=[dict(x=(isotodatetime(p.timestamp) - end_time).seconds / (60.0 * 60), y=p.price / scale) for p in prices],
    #     name="%s %s" % (zone, instance_type),
    #     zone="%s" % (zone),
    #     itype="%s" % (instance_type),
    #     color="lightblue")

    file_path = task_dir+"/proc_stats.txt"
    cpu_series, mem_series = read_task_profile(config['TARGET_ROOT'] + "/" + run_name + "/files", file_path=file_path)
    return flask.render_template("task_profile.html", run_name=run_name, cpu_series=cpu_series, mem_series=mem_series)

@app.route("/prices")
def show_prices():
    ec2 = get_ec2_connection()

    series = []
    for zone in ["us-east-1a", "us-east-1b", "us-east-1c"]:
        for t, s in cpus_per_instance.items():
            if t.startswith("r"):
                series.append(prices.get_price_series(ec2, t, s, zone))

    prices.normalize_series(series)
    per_instance_price = {}
    for s in series:
        values = [x["y"] for x in s["data"]]
        per_instance_price[s["name"]] = (s["zone"], s["itype"],prices.median(values), values[-1])
    per_instance_price = [(n,v[0], v[1], v[2], v[3]) for n,v in per_instance_price.items()]
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
    global monitor_parameters

    monitor_parameters = cluster_monitor.Parameters(config['INSTANCE_TYPE'], config['IGNORE_GRP'])

    log_file = None

    if "MONITOR_LOG" in config:
        log_file = open(config["MONITOR_LOG"], "a")

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
                                                     get_wingman_service_factory(),
                                                     config["MONITOR_JSON_LOG"])

if __name__ == "__main__":
    import trace_on_demand
    trace_on_demand.install()

    parser = argparse.ArgumentParser(description='Start webserver for cluster ui')
    parser.add_argument('--config', dest="config_path", help='config file to use', default=os.path.expanduser("~/.clusterui.config"))
    args = parser.parse_args()

    app.config.update(LOG_FILE='clusterui.log',
                      SUBMISSION_HISTORY="submission_history.shelve",
                      DEBUG=True,
                      PORT=9935,
                      FLOCK_PATH="/xchip/flock/bin/phlock",
                      LOADBALANCE_PID_FILE="loadbalance.pid",
                      TARGET_ROOT = "/data2/runs",
                      INSTANCE_TYPE = "r3.2xlarge",
                      IGNORE_GRP = False,
                      MONITOR_JSON_LOG="monitor-log.json")
    app.config.from_pyfile(args.config_path)
    app.config.update(OPENID_FS_STORE_PATH="/tmp/openid-clusterui-"+app.config['CLUSTER_NAME'])
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
