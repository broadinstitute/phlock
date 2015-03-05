__author__ = 'pmontgom'

from jinja2 import Template
import math

import adhoc

def make_flock_configs(config_defs, template_str, timestamp, defaults):
    template = Template(template_str)
    json_and_flock = []

    configs = adhoc.enumerate_configurations(config_defs)
    job_id_format = "%%s-%%0%dd" % math.ceil(math.log(len(configs))/math.log(10))
    job_index = 0

    for config in configs:
        config.update(defaults)
        config["run_id"] = job_id_format % (timestamp, job_index)
        flock = template.render(config)
        json_and_flock.append( (config, flock) )
        job_index += 1

    return json_and_flock

