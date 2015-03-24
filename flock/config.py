import collections
import logging
import os
import base64
import hashlib
import re

log = logging.getLogger("flock")

Config = collections.namedtuple("Config", ["base_run_dir", "executor", "invoke", "bsub_options", "qsub_options",
                                           "scatter_bsub_options", "scatter_qsub_options", 
                                           "gather_bsub_options", "gather_qsub_options",
                                           "workdir", "name", "run_id",
                                           "wingman_host",
                                           "wingman_port", "environment_variables", "language"])

def parse_config(f, multivalue_keys):
    props = {}
    line_no = 0
    while True:
        line = f.readline()
        line_no += 1

        if line == "":
            break

        # ignore comments
        if line.strip().startswith("#") or len(line.strip()) == 0:
            continue

        # parse prop name and value
        colon_pos = line.find(":")
        if colon_pos < 0:
            raise Exception("Did not find ':' in line %d" % line_no)
        prop_name = line[:colon_pos].strip()
        prop_value = line[colon_pos + 1:].strip()

        # handle quoted values
        if len(prop_value) > 0 and prop_value[0] in ["\"", "'"]:
            if len(prop_value) <= 1 or prop_value[-1] != prop_value[0]:
                raise Exception("Could not find end of quoted string on line %d" % line_no)
            prop_value = prop_value[1:-1].decode("string-escape")

        # check to see if this was "invoke" property, in which case, consume the rest of the file as the script
        # with no escaping
        if prop_name == "invoke":
            prop_value = f.read()

        if prop_name in multivalue_keys:
            if not (prop_name in props):
                props[prop_name] = []
            props[prop_name].append(prop_value)
        else:
            props[prop_name] = prop_value

    return props


def load_config(filenames, run_id, overrides):
    config = {"bsub_options": "", "qsub_options": "", "workdir": ".", "name": "", "base_run_dir": ".", "wingman_host":None, "wingman_port":3010, "setenv":[], "language": "R"}
    for filename in filenames:
        log.info("Reading config from %s", filename)
        with open(filename) as f:
            config.update(parse_config(f, ["setenv"]))

    if not ( "scatter_bsub_options" in config ):
        config["scatter_bsub_options"] = config["bsub_options"]
    
    if not ( "gather_bsub_options" in config ):
        config["gather_bsub_options"] = config["bsub_options"]

    if not ( "scatter_qsub_options" in config ):
        config["scatter_qsub_options"] = config["qsub_options"]

    if not ( "gather_qsub_options" in config ):
        config["gather_qsub_options"] = config["qsub_options"]

    if re.search("(?:^|\\s+)-p -?\\d+", config["qsub_options"]) == None:
        # This is a bit of a hack:  Try to see if a priority has been manually specified.  If not, specify it.
        # if the scatter qsub options are not set, then default to scatter jobs getting
        # a higher priority.  However, it appears non-admins can't submit jobs with >0 priority
        # so instead, make all other jobs lower priority
        config["qsub_options"] = config["qsub_options"] + " -p -5"

    if not ("run_id" in config):
        assert config['base_run_dir'] != None
        assert run_id != None
        config['run_id'] = os.path.abspath(os.path.join(config['base_run_dir'], os.path.basename(run_id)))

    if not ("name" in config):
        # make a hash of the run directory that will likely be unique so that we can distinguish which job is which
        config['name'] = base64.urlsafe_b64encode(hashlib.md5(config['run_id']).digest())[0:10]

    config.update(overrides)

    assert "base_run_dir" in config
    assert "executor" in config
    assert "invoke" in config
    
    setenv = config['setenv']
    del config['setenv']
    config['environment_variables'] = setenv

    return Config(**config)
