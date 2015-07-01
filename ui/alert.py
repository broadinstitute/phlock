__author__ = 'pmontgom'

import time
import boto.sns
import threading

def send_alert(region, key, secret, topic, message):
    snsc = boto.sns.connect_to_region(region,
       aws_access_key_id=key,
       aws_secret_access_key=secret)

    return snsc.publish(topic, message, "Notification from cluster's deadman's switch")

class Alerter:
    def __init__(self, key, secret, region, topic, min_minutes_between_msgs=10):
        self.last_sent = {}
        self.min_minutes_between_msgs = min_minutes_between_msgs
        self.key = key
        self.secret = secret
        self.region = region
        self.topic = topic
        self.lock = threading.Lock()

    def alert(self, name, details):
        message = "%s: %s" % (name, details)
        #print "Sending alert:", message

        with self.lock:
            last_time = self.last_sent.get(name)
            now = time.time()
            if last_time is None or now > (last_time + self.min_minutes_between_msgs * 60):
                send_alert(self.region, self.key, self.secret, self.topic, message)
                self.last_sent[name] = now
