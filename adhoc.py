__author__ = 'pmontgom'

import collections

def project(columns, record):
    row = []
    for n in columns:
        if n in record:
            v = record[n]
            if type(v) == list or type(v) == tuple:
                v = tuple(v)
        else:
            v = None
        row.append(v)
    return tuple(row)

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

def index_by_columns(columns, values):
    values_by_idx = collections.defaultdict(lambda: [])
    for e in values:
        k = project(columns, e)
        values_by_idx[k].append(e)
    return values_by_idx


def find_each_in_a(columns, values_a, values_b):
    # returns list of (key, value from a, list of values from b)
    idx_a = index_by_columns(columns, values_a)
    idx_b = index_by_columns(columns, values_b)

    rows = []
    items = idx_a.items()
    items.sort()
    for k, a in items:
        assert len(a) == 1
        a = a[0]
        rows.append((k, a, idx_b[k]))
    assert len(idx_a) == len(values_a)
    assert len(values_a) == len(rows)
    return rows

def create_job_tag_set(job, keys=None):
    tags = set()
    if keys == None:
        keys = job.keys()
    for k in keys:
        if not (k in job):
            continue
        v = job[k]
        if type(v) == list:
            v = " ".join([str(x) for x in v])
        if not ( k in ["job_name", "job_dir", "status", "run_id"] ):
            tags.add("%s=%s" % (k, str(v)))
    return tags


def summarize(jobs):
    " returns a map of key -> set of values seen "
    values_per_key = collections.defaultdict(lambda:set())
    for job in jobs:
        for k, v in job.parameters.items():
            if type(v) == list:
                v = tuple(v)
            values_per_key[k].add(v)
    return values_per_key

def factor_out_common(summary):
    # given map of key to unique values, return a map of those that mapped to a single value and a list of columns for others
    fixed = {}
    columns = []
    for k, values in summary.items():
        if len(values) == 1:
            fixed[k] = list(values)[0]
        else:
            columns.append(k)
    return fixed, columns

def get_all_tags(summary):
    tags = []
    for k, values in summary.items():
        for v in values:
            if type(v) == list or type(v) == tuple:
                v = " ".join([str(x) for x in v])
            tags.append("%s=%s" % (k,str(v)))
    return tags

class Job:
    def __init__(self, name, parameters, status):
        self.parameters = parameters
        self.tags = create_job_tag_set(parameters)
        self.name = name
        self.status = status

    def has_tags(self, tags):
        for tag in tags:
            if not (tag in self.tags):
                return False
        return True

def flatten(records, existing_jobs_status):
    # returns list of jobs
    jobs = []
    for k, ref, existing_jobs in records:
        if len(existing_jobs) == 0:
            jobs.append(Job(None, ref, None))
        else:
            for existing_job in existing_jobs:
                run_id = existing_job["run_id"]
                status = existing_jobs_status[run_id]
                jobs.append(Job(run_id, existing_job, existing_jobs_status[run_id]))
    return jobs

def from_existing(existing_jobs):
    # returns list of jobs
    jobs = []
    for existing_job in existing_jobs:
        p = existing_job["parameters"]
        jobs.append(Job(p["run_id"], p, existing_job['status']))
    return jobs


def find_with_tags(jobs, tags):
    return [job for job in jobs if job.has_tags(tags)]
