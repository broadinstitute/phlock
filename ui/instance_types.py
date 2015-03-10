__author__ = 'pmontgom'

import collections

import awsInstanceTypes

instance_sizes = awsInstanceTypes.getInstanceTypes()
instance_sizes.sort(lambda a, b: -cmp(a[1], b[1]))

# return a ridiculous number for any instances we don't know about.  Better then crashing.  Need to address this later.
cpus_per_instance = dict()
for instance_type, cpus in instance_sizes:
    cpus_per_instance[instance_type] = cpus
