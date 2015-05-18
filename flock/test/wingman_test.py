import flock.wingman as wingman
import os
import tempfile
import shutil
from nose import with_setup

__author__ = 'pmontgom'

temp_db = None
temp_dir = None
run_dir = None
config_path = None

SAMPLE_CONFIG="""
executor: wingman
invoke:
...R code would go here...
"""

def setup_run_dir():
    global run_dir, config_path, temp_dir
    temp_dir = tempfile.mkdtemp()
    run_dir = os.path.join(temp_dir, "files")

    # mock up a run_dir with a single task
    config_path = os.path.join(temp_dir, "config")
    with open(config_path, "w") as fd:
        fd.write(SAMPLE_CONFIG)

    global temp_db
    temp_db = tempfile.NamedTemporaryFile().name
    print "temp_db = %s" % temp_db

def cleanup_run_dir():
    global temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    temp_dir = None

    global temp_db
    if os.path.exists(temp_db):
        os.unlink(temp_db)
    print "deleting %s" % temp_db
    temp_db = None

@with_setup(setup_run_dir, cleanup_run_dir)
def test_archive_run():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    run = store.run_submitted(run_dir, "name", config_path, "{}")
    assert len(store.get_runs()) == 1

    assert not ("test_archive" in store.list_archives())

    # now, archive it
    new_dir = store.archive_run("name", "test_archive")
    assert len(store.get_runs()) == 0
    assert len(store.get_runs("test_archive")) == 1

    # make sure we got the config file
    assert os.path.exists(os.path.join(new_dir, "config"))

    print "get_run", store.get_run("name")

    assert store.get_run("name")["run_dir"] == new_dir
    assert ("test_archive" in store.list_archives())


@with_setup(setup_run_dir, cleanup_run_dir)
def test_set_tag():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    # create a run which we can manipulate
    store.run_submitted(run_dir, "name", config_path, "{}")

    store.set_tag("name", "prop1", "1")

    assert store.get_run("name")["added_tags"]["prop1"] == "1"

    store.set_tag("name", "prop1", "2")

    assert store.get_run("name")["added_tags"]["prop1"] == "2"

@with_setup(setup_run_dir, cleanup_run_dir)
def test_successful_run_lifecycle():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    assert len(store.get_runs()) == 0

    # simulate submission of the run to wingman
    run = store.run_submitted(run_dir, "name", config_path, "{}")
    full_task_dir_paths = run['task_dirs']

    # make sure we got the task directory for the task within that run
    assert len(full_task_dir_paths) == 1
    task_dir = full_task_dir_paths[0]

    # at this point, we expect a single run, with a single task which is ready for submission
    runs = store.get_runs()
    assert len(runs) == 1
    assert runs[0]['status']['WAITING'] == 1

    # also make sure the detailed version works
    tasks = store.get_run_tasks(runs[0]['run_dir'])
    assert len(tasks) == 1

    # notify the store that the task started and then completed successfully
    store.task_started(task_dir, "node01")
    runs = store.get_runs()
    assert runs[0]['status']['STARTED'] == 1

    store.task_completed(task_dir)
    runs = store.get_runs()
    assert runs[0]['status']['COMPLETED'] == 1

    # now make sure we can clean up the run by deleting it
    store.delete_run(run_dir)
    assert len(store.get_runs()) == 0

@with_setup(setup_run_dir, cleanup_run_dir)
def test_failed_run_lifecycle():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    # simulate submission of the run to wingman
    run = store.run_submitted(run_dir, "name", config_path, "{}")
    full_task_dir_paths = run['task_dirs']
    task_dir = full_task_dir_paths[0]

    # at this point, we expect a single run, with a single task which is ready for submission
    runs = store.get_runs()
    assert len(runs) == 1
    assert runs[0]['status']['WAITING'] == 1

    # notify the store that the task started
    store.task_started(task_dir, "node01")
    runs = store.get_runs()
    assert runs[0]['status']['STARTED'] == 1

    # and then fails
    store.task_failed(task_dir)
    runs = store.get_runs()
    assert runs[0]['status']['FAILED'] == 1

    store.retry_run(run_dir)
    runs = store.get_runs()
    print runs
    assert runs[0]['status']['WAITING'] == 1


@with_setup(setup_run_dir, cleanup_run_dir)
def test_node_failure():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    # simulate submission of the run to wingman
    run = store.run_submitted(run_dir, "name", config_path, "{}")
    full_task_dir_paths = run['task_dirs']
    task_dir = full_task_dir_paths[0]

    # at this point, we expect a single run, with a single task which is ready for submission
    runs = store.get_runs()
    store.task_started(task_dir, "node01")

    runs = store.get_runs()
    print runs
    assert runs[0]['status']['STARTED'] == 1

    # and then node fails
    store.node_disappeared("node01")

    # confirm state switched back to WAITING
    runs = store.get_runs()
    assert runs[0]['status']['WAITING'] == 1

@with_setup(setup_run_dir, cleanup_run_dir)
def test_file_ops():
    store = wingman.TaskStore(temp_db, "flock_home", endpoint_url="http://invalid:2000")

    store.run_submitted(run_dir, "name", config_path, "{}")

    # write a sample file in the run directory
    with open(os.path.join(run_dir, "sample"), "w") as fd:
        fd.write("test-text")

    files = store.get_run_files("name", "*")
    print files
    found_dir = False
    found_file = False
    for x in files:
        if x["name"] == "tasks":
            assert x["is_dir"]
            found_dir = True
        if x["name"] == "sample":
            assert not x["is_dir"]
            found_file = True

    assert found_dir
    assert found_file

    import base64
    file_content = store.get_file_content("name", "sample", 0, 10000)
    assert base64.standard_b64decode(file_content['data']) == "test-text"
