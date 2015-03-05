__author__ = 'pmontgom'

import cluster_monitor
import mock

next_time = 1000

def mock_time():
    return next_time

def advance_time(delta):
    global next_time
    next_time += delta

@mock.patch("cluster_monitor.time.time", mock_time)
def test_timer():
    t = cluster_monitor.Timer(20)
    assert t.time_remaining == 20
    assert not t.expired

    advance_time(10)
    assert t.time_remaining == 10
    assert not t.expired

    advance_time(10)
    assert t.time_remaining == 0
    assert t.expired

    advance_time(10)
    assert t.time_remaining == 0
    assert t.expired

    t.reset()
    assert t.time_remaining == 20
    assert not t.expired

def test_mailbox():
    mailbox = cluster_monitor.Mailbox()

    mailbox.send("b")
    mailbox.send("a")

    msg = mailbox.wait_for(["c"], timeout=0)
    assert msg == None

    msg = mailbox.wait_for(["a"], timeout=0)
    assert msg == "a"

    msg = mailbox.wait_for(["b"], timeout=0)
    assert msg == "b"

    msg = mailbox.wait_for(["a"], timeout=0)
    assert msg == None

