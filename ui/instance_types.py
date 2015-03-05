__author__ = 'pmontgom'

import collections

instance_sizes = [("c3.large", 2), ("c3.xlarge", 4), ("c3.2xlarge", 8), ("c3.4xlarge", 16), ("c3.8xlarge", 32),
                  ("r3.large", 2), ("r3.xlarge", 4), ("r3.2xlarge", 8), ("r3.4xlarge", 16), ("r3.8xlarge", 32)]

instance_sizes.sort(lambda a, b: -cmp(a[1], b[1]))

# return a ridiculous number for any instances we don't know about.  Better then crashing.  Need to address this later.
cpus_per_instance = collections.defaultdict(lambda: 10000)
for instance_type, cpus in instance_sizes:
    cpus_per_instance[instance_type] = cpus
cpus_per_instance['m3.medium'] = 1
cpus_per_instance['m3.large'] = 2
cpus_per_instance['m3.xlarge'] = 4
cpus_per_instance['m3.2xlarge'] = 8
