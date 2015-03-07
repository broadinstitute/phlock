from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log
import subprocess
import time
import os
import glob
import boto.sdb
import boto.exception
from starcluster.static import SECURITY_GROUP_PREFIX
import tempfile
from string import Template
import tempfile

class InstallScripts(ClusterSetup):
  def install_scripts(self, node):
    scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
    
    node.ssh.execute("if [ ! -e /tmp/cluster_scripts ] ; then mkdir /tmp/cluster_scripts ; fi")
    node.ssh.put(glob.glob(scripts_dir+"/*"), "/tmp/cluster_scripts")
    node.ssh.execute("chmod a+x /tmp/cluster_scripts/*")
    
  def run(self, nodes, master, user, user_shell, volumes):
    for node in nodes:
      self.install_scripts(node)
  
  def on_add_node(self, node, nodes, master, user, user_shell, volumes):
    self.install_scripts(node)

class AddTaigaAlias(ClusterSetup):
  def __init__(self):
    pass
  def run(self, nodes, master, user, user_shell, volumes):
    #tunnel_ip = master.network_names['INTERNAL_IP']
    #for node in nodes:
    #  log.info("Updating datasci-dev alias in /etc/hosts %s" % node)
    #  node.ssh.execute("cp /etc/hosts /tmp/etc_hosts ; (grep -v datasci-dev /tmp/etc_hosts ; echo %s datasci-dev) > /tmp/new_hosts" % tunnel_ip)
    update_etc_hosts(master)

  def on_add_node(self, node, nodes, master, user, user_shell, volumes):
    log.warn("starting shell")
    node.ssh.execute("sudo -u sgeadmin ssh -L8999:localhost:8999 master -fN > /tmp/tunnel.stdout 2> /tmp/tunnel.stderr")
    log.warn("setting up etc/hosts")
    update_etc_hosts(node)
    log.warn("done")

class SetupSgeMemoryConstraints(ClusterSetup):
  def __init__(self):
    pass
  
  def update_sge_config(self, master, nodes):
    params = " ".join(["%s %s" % (node.alias, node.instance_type) for node in nodes]) 
    command = "/tmp/cluster_scripts/update_sge_master.py "+params
    log.warn("executing %s" % repr(command))
    master.ssh.execute(command)

  def run(self, nodes, master, user, user_shell, volumes):
    self.update_sge_config(master, nodes)
  
  def on_add_node(self, node, nodes, master, user, user_shell, volumes):
    log.warn("node=%s, nodes=%s" % (node, nodes))
    self.update_sge_config(master, [node])

class AddInitializedTag(ClusterSetup):
  def __init__(self):
    pass
  
  def add_tag(self, master, nodes):
    for node in nodes:
      node.add_tag("initialized", "True")

  def run(self, nodes, master, user, user_shell, volumes):
    self.add_tag(master, nodes)
  
  def on_add_node(self, node, nodes, master, user, user_shell, volumes):
    log.warn("AddInitializedTag node=%s, nodes=%s" % (node, nodes))
    self.add_tag(master, [node])

class AddDeadmansSwitchCrontab(ClusterSetup):
  def __init__(self, key, secret, topic, region='us-east-1'):
    self.key = key
    self.secret = secret
    self.topic = topic
    self.region = region
  
  def setup_crontab(self, master, nodes):
    cluster_name = master.parent_cluster.name
    assert cluster_name.startswith(SECURITY_GROUP_PREFIX)
    cluster_name = cluster_name[len(SECURITY_GROUP_PREFIX):]
    
    domain = '%s-heartbeats' % cluster_name
    # make sure that the domain exists and the user can access it
    sdbc = boto.sdb.connect_to_region(self.region, aws_access_key_id=self.key, aws_secret_access_key=self.secret)
    assert sdbc != None
    try:
      dom = sdbc.get_domain(domain)
    except boto.exception.SDBResponseError:
      log.warn("Creating new domain %s for heartbeats", domain)
      dom = sdbc.create_domain(domain)
    log.warn("Verifying that domain %s is accessible with non-admin credentials", domain)
    item = dom.get_item('heartbeat')
    
    script_template_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "deadmanswitch-check.template")
    script_template = open(script_template_name).read()
    
    # apply config settings to template
    script_body = script_template.format(**dict(key=self.key,
      secret=self.secret,
      topic=self.topic,
      domain=domain,
      region=self.region,
      cluster_name=cluster_name))
    
    script = tempfile.NamedTemporaryFile("w")
    script.write(script_body)
    script.flush()
    
    for node in nodes:
      node.ssh.put(script.name, "/tmp/cluster_scripts/deadmanswitch-check.py")
      node.ssh.execute("chmod a+xr /tmp/cluster_scripts/deadmanswitch-check.py")
      log.warn("Adding cronjob for checking deadmans switch on %s", str(node))
      command = "echo '0,10,20,30,40,50 * * * * ubuntu /usr/bin/python /tmp/cluster_scripts/deadmanswitch-check.py > /tmp/deadmanswitch-check.log 2>&1' > /etc/cron.d/cluster-deadmans-switch && service cron reload" 
      node.ssh.execute(command)

  def run(self, nodes, master, user, user_shell, volumes):
    self.setup_crontab(master, nodes)
  
  def on_add_node(self, node, nodes, master, user, user_shell, volumes):
    self.setup_crontab(master, [node])

class InstallWingmanService(ClusterSetup):
  def __init__(self, flock_home, run_dir):
    self.flock_home = flock_home
    self.run_dir = run_dir
  
  def run(self, nodes, master, user, user_shell, volumes):
    wingman_script_template = Template("""#!/bin/bash
source /etc/profile.d/sge.sh
exec ${flock_home}/bin/phlock-wingman sge ${run_dir}/jobdb.sqlite3 3010 --maxsubmitted 10000
""")
    
    temp_wingman_script = tempfile.NamedTemporaryFile()
    temp_wingman_script.write(wingman_script_template.substitute(flock_home=self.flock_home, run_dir=self.run_dir))
    temp_wingman_script.flush()
    
    wingman_conf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wingman.conf")
    master.ssh.put(wingman_conf_path, "/etc/init/wingman.conf")
    master.ssh.put(temp_wingman_script.name, "/tmp/cluster_scripts/start_wingman.sh")
    master.ssh.execute("chmod +x /tmp/cluster_scripts/start_wingman.sh")
    master.ssh.execute("start wingman")
  