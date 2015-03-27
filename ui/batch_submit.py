__author__ = 'pmontgom'

from jinja2 import Template
import math

import adhoc
import ui
import re

def override_settings(flock_config, overrides):
    lines = flock_config.split("\n")
    in_header=True
    xlines = []
    for line in lines:
        if in_header:
            m = re.match("\\s*([A-Za-z][^ :]+)\\s*:\\s*(.*)", line)
            if m != None:
                name = m.group(1).strip()
#                value = m.group(2).strip()
                if name in overrides:
                    continue
                if name == "invoke":
                    for name, value in overrides.items():
                        xlines.append("%s: %s" % (name, value))
                    in_header = False
        xlines.append(line)
    return "\n".join(xlines)


def make_flock_configs(config_defs, template_str, timestamp, defaults):
    template = Template(template_str)
    json_and_flock = []

    configs = adhoc.enumerate_configurations(config_defs)
    job_id_format = "%%s-%%0%dd" % math.ceil(math.log(len(configs))/math.log(10))
    job_index = 0

    for config in configs:
        config.update(defaults)
        run_id = job_id_format % (timestamp, job_index)
        config["run_id"] = run_id
        flock = template.render(config)

        overrides = {
            "run_id": ui.get_run_files_path(run_id),
            "executor": "flock",
            "name": run_id
        }

        if "sha" in config:
            sha = config["sha"]
            overrides["workdir"] = ui.get_code_cache_dir(sha)

        flock = override_settings(flock, overrides)

        json_and_flock.append( (config, flock) )
        job_index += 1

    return json_and_flock

