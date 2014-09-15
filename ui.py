import flask
from flask import request
import subprocess
import functools
import formspec
import tempfile
import re
import term
import json

from flask.ext.openid import OpenID

import boto
import collections
import ConfigParser, os
import boto.ec2

#import traceback
from werkzeug.debug import tbtools

import cluster_monitor
import argparse
import logging
import prices
from instance_types import cpus_per_instance, instance_sizes

oid = OpenID(None, "/tmp/clusterui-openid")
terminal_manager = term.TerminalManager()

from werkzeug.local import LocalProxy
def _get_current_config():
    return flask.current_app.config

config = LocalProxy(_get_current_config)

def load_starcluster_config(app_config):
    config = ConfigParser.ConfigParser()
    config.read(app_config['STARCLUSTER_CONFIG'])
    app_config['AWS_ACCESS_KEY_ID'] = config.get("aws info", "AWS_ACCESS_KEY_ID")
    app_config['AWS_SECRET_ACCESS_KEY'] = config.get("aws info", "AWS_SECRET_ACCESS_KEY")
    default_template = config.get("global", "DEFAULT_TEMPLATE")
    key_name = config.get("cluster %s" % default_template, "KEYNAME")
    app_config['KEY_LOCATION'] = os.path.expanduser(config.get("key %s" % key_name, "KEY_LOCATION"))

def get_ec2_connection():
    return boto.ec2.connection.EC2Connection(aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                                        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'])

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



def get_instance_counts(ec2):
    instances = ec2.get_only_instances()
    instances = filter_inactive_instances(instances)
    counts = collections.defaultdict(lambda: 0)
    for i in instances:
        counts[i.instance_type] += 1

    return counts


def run_command(args):
    terminal = terminal_manager.start_term(args)
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

@app.route("/")
def index():
    ec2 = get_ec2_connection()
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

    state = cluster_manager.get_state()
    print "state=%s" % state

    return flask.render_template("index.html", terminals=active_terminals, instances=instance_table, state=state)


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
@app.route("/terminate")
def terminate_cluster():
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


TARGET_ROOT = "/data2/runs"


@app.route("/list-jobs")
def list_jobs():
    p = subprocess.Popen([config['STARCLUSTER_CMD'], "sshmaster", config['CLUSTER_NAME'], TARGET_ROOT + "/get_runs.py " + TARGET_ROOT],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    stdout, stderr = p.communicate()
    jobs = json.loads(stdout)
    return flask.render_template("list-jobs.html", jobs=jobs,
                                 column_names=["status", "celllineSubset", "targetDataset", "predictiveFeatureSubset",
                                               "targetDataType", "predictiveFeatures"])

job_pattern = re.compile("\\d+-\\d+")

@app.route("/pull-job")
@secured
def pull_job():
    job_name = request.values["job"]
    assert job_pattern.match(job_name) != None
    t = tempfile.NamedTemporaryFile(delete=False)
    t.write("set +ex\n")
    t.write("cd /xchip/datasci/ec2-runs\n")
    t.write("%s sshmaster %s --user ubuntu /xchip/scripts/make_model_summaries.R /data2/runs/%s/files/results\n" % (
        config['STARCLUSTER_CMD'], config['CLUSTER_NAME'], job_name))
    t.write("%s sshmaster %s --user ubuntu python /xchip/scripts/bundle_run_dirs.py /data2/runs/%s | tar xvzf -\n" % (
        config['STARCLUSTER_CMD'], config['CLUSTER_NAME'], job_name))
    t.close()
    return run_command(["bash", t.name])


@app.route("/retry-job")
@secured
def retry_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_starcluster_cmd(["sshmaster", config['CLUSTER_NAME'], "--user", "ubuntu",
                                "bash " + TARGET_ROOT + "/" + job_name + "/flock-wrapper.sh retry"])


@app.route("/check-job")
@secured
def check_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_starcluster_cmd(["sshmaster", config['CLUSTER_NAME'], "--user", "ubuntu",
                                "bash " + TARGET_ROOT + "/" + job_name + "/flock-wrapper.sh check"])


@app.route("/poll-job")
@secured
def poll_job():
    job_name = request.values["job"]
    assert not ("/" in job_name)
    return run_starcluster_cmd(["sshmaster", config['CLUSTER_NAME'], "--user", "ubuntu",
                                "bash " + TARGET_ROOT + "/" + job_name + "/flock-wrapper.sh poll"])




@app.route("/syncruns")
@secured
def syncRuns():
    ec2 = get_ec2_connection()
    master, key_location = get_master_info(ec2)
    return run_command([config['PYTHON_EXE'], "-u", "syncRuns.py", master.dns_name, key_location])


@app.route("/submit-generic-job-form")
@secured
def submit_generic_job_form():
    return flask.render_template("submit-job-form.html", form=formspec.GENERIC_FORM)


@app.route("/submit-job-form")
@secured
def submit_job_form():
    return flask.render_template("submit-job-form.html", form=formspec.ATLANTIS_FORM)


@app.route("/submit-job", methods=["POST"])
@secured
def submit_job():
    template = request.values['template']
    matching_forms = [f for f in [formspec.ATLANTIS_FORM, formspec.GENERIC_FORM] if f.template == template]
    assert len(matching_forms) == 1
    f = matching_forms[0]

    params = parse_reponse(f.fields, request.values, request.files)
    flock_config = formspec.apply_parameters(f.template, params)

    if "download" in request.values:
        response = flask.make_response(flock_config)
        response.headers["Content-Disposition"] = "attachment; filename=config.flock"
        return response
    else:
        ec2 = get_ec2_connection()
        master, key_location = get_master_info(ec2)

        t = tempfile.NamedTemporaryFile(delete=False)
        t.write(flock_config)
        t.close()

        t2 = tempfile.NamedTemporaryFile(delete=False)
        t2.write(json.dumps(params))
        t2.close()

        return run_command(
            [config['PYTHON_EXE'], "-u", "remoteExec.py", master.dns_name, key_location, params["repo"], params["branch"], t.name,
             TARGET_ROOT, t2.name])


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
    monitor_parameters.time_per_job=int(values['time_per_job'])
    monitor_parameters.time_to_add_servers_fixed=int(values['time_to_add_servers_fixed'])
    monitor_parameters.time_to_add_servers_per_server=int(values['time_to_add_servers_per_server'])
    monitor_parameters.instance_type=values['instance_type']
    monitor_parameters.domain=values['domain']
    monitor_parameters.jobs_per_server=int(values['jobs_per_server'])

    return flask.redirect("/")

@app.route("/start-tunnel")
@secured
def start_tunnel():
    ec2 = get_ec2_connection()

    master, key_location = get_master_info(ec2)

    return run_command(["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
                        "-i", key_location, "-R 8999:datasci-dev.broadinstitute.org:8999", "-N",
                        "ubuntu@" + master.dns_name])


@app.route("/test-run")
@secured
def test_run():
    return run_command(["bash", "-c", "while true ; do echo line ; sleep 1 ; date ; done"])

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

    cluster_terminal = terminal_manager.start_named_terminal("Cluster monitor")
    cluster_manager = cluster_monitor.ClusterManager(monitor_parameters, config['CLUSTER_NAME'], cluster_terminal, [config['STARCLUSTER_CMD'], "-c", config['STARCLUSTER_CONFIG']])
    cluster_manager.start_manager()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start webserver for cluster ui')
    parser.add_argument('--config', dest="config_path", help='config file to use', default=os.path.expanduser("~/.clusterui.config"))
    args = parser.parse_args()

    app.config.update(LOG_FILE='clusterui.log', DEBUG=True, PORT=9935)
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
