__author__ = 'pmontgom'

from jinja2 import Template
import math

def cartesian_product(options):
    k = options.keys()[0]
    rows = []
    for v in options[k]:
        rows.append({k:v})

    if len(options) == 1:
        return rows
    else:
        remaining = dict(options)
        del remaining[k]
        other_rows = cartesian_product(remaining)
        merged_rows = []

        for a in rows:
            for b in other_rows:
                d = {}
                d.update(a)
                d.update(b)
                merged_rows.append(d)
        return merged_rows

def enumerate_configurations(configs):
    rows = []
    for config in configs:
        rows.extend(cartesian_product(config))
    return rows


def make_flock_configs(config_defs, template_str, timestamp, defaults):
    template = Template(template_str)
    json_and_flock = []

    configs = enumerate_configurations(config_defs)
    job_id_format = "%%s-%%0%dd" % math.ceil(math.log(len(configs))/math.log(10))
    job_index = 0

    for config in configs:
        config.update(defaults)
        config["run_id"] = job_id_format % (timestamp, job_index)
        flock = template.render(config)
        json_and_flock.append( (config, flock) )
        job_index += 1

    return json_and_flock

