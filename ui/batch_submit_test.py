__author__ = 'pmontgom'

import batch_submit

def test_override_settings():
    config = """
# config
run_id: /data2/runs/20150327-154402-0/files
executor: flock
setenv: R_LIBS=/data2/Rpackages
workdir: /data2/code-cache/f04d537fed3be9d4faa3c0db3551f571c2891880
name: 20150327-154402-0

scatter_qsub_options: -l h_vmem=8G
qsub_options: -l h_vmem=2G -p -5

invoke:|
  stopifnot(flock_version[1] == 1)
"""
    final_config = batch_submit.override_settings(config, {"workdir":"/home"})

    assert final_config == """
# config
run_id: /data2/runs/20150327-154402-0/files
executor: flock
setenv: R_LIBS=/data2/Rpackages
name: 20150327-154402-0

scatter_qsub_options: -l h_vmem=8G
qsub_options: -l h_vmem=2G -p -5

workdir: /home
invoke:|
  stopifnot(flock_version[1] == 1)
"""
