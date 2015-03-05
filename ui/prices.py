__author__ = 'pmontgom'

import datetime

def isotodatetime(t):
    if t.endswith("Z"):
        t = t[:-1]
    return datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")


colors = ["#7fc97f",
          "#beaed4",
          "#fdc086",
          "#ffff99",
          "#386cb0",
          "#f0027f",
          "#bf5b17",
          "#666666"]


def get_price_series(ec2, instance_type, scale, zone="us-east-1a"):
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(days=1)

    prices = ec2.get_spot_price_history(start_time=start_time.isoformat(), end_time=end_time.isoformat(),
                                        instance_type=instance_type, product_description="Linux/UNIX",
                                        availability_zone=zone, max_results=None, next_token=None)

    result = dict(
        data=[dict(x=(isotodatetime(p.timestamp) - end_time).seconds / (60.0 * 60), y=p.price / scale) for p in prices],
        name="%s %s" % (zone, instance_type),
        color="lightblue")

    result["data"].sort(lambda a, b: cmp(a["x"], b["x"]))

    return result


def normalize_series(series):
    def bucket(v):
        return int(v / 0.05) * 0.05

    all_x_values = set()
    for s in series:
        all_x_values.update([bucket(x["x"]) for x in s['data']])

    all_x_values = list(all_x_values)
    all_x_values.sort()

    def normalize(values):
        new_values = []

        xi = 0
        y = None
        for i in xrange(len(values)):
            v = values[i]
            x = bucket(v["x"])
            y = v["y"]

            while xi < len(all_x_values) and all_x_values[xi] < x:
                new_values.append(dict(x=all_x_values[xi], y=y))
                xi += 1
            new_values.append(dict(x=x, y=y))
        while xi < len(all_x_values) and y != None:
            new_values.append(dict(x=all_x_values[xi], y=y))
            xi += 1

        return new_values

    for s in series:
        s["data"] = normalize(s["data"])


def median(values):
    values = list(values)
    values.sort()
    return values[len(values) / 2]
