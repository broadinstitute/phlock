import sys
import collections
import time
import os
import subprocess
import signal

Stats = collections.namedtuple("Stats", ["timestamp", "utime", "stime", "starttime", "vsize", "rss"])

clock_ticks_per_second = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
page_size = os.sysconf('SC_PAGESIZE')
bytes_per_mb = 1000*1000
def read_file(fn):
  content = None
  with open(fn) as fd:
    content = fd.read()
  return content

def read_stats(pid):
  content = read_file("/proc/%s/stat" % pid)
  if content == None:
    return None
  fields = content.split(" ")
  # do I need to worry about spaces in 'comm'?
  # sanity check: make sure state is a single character, suggesting we're probably lined up right
  assert len(fields[2]) == 1
  utime = int(fields[13])
  stime = int(fields[14])
  starttime = int(fields[21])
  vsize = int(fields[22])
  rss = int(fields[23])
  
  return Stats(int(time.time()), utime/clock_ticks_per_second, stime/clock_ticks_per_second, starttime/clock_ticks_per_second, vsize/bytes_per_mb, rss*page_size/bytes_per_mb)

def write_stats(fd, pid):
  stats = read_stats(pid)
  if stats != None:
    fd.write("r %s %s %s %s %s\n" % ( stats.timestamp, stats.utime, stats.stime, stats.vsize, stats.rss )) 
  else:
    fd.write("m NA NA NA NA NA\n")
  fd.flush()

class Timeout(Exception):
  pass

def adjust_omm_score(pid):
  try: 
    fd = open("/proc/%s/oom_score_adj" % pid, "w")
    fd.write("1000")
    fd.close()
  except IOError:
    pass

def run_and_watch(log_file, args, delay_between_checks=10):
  if os.path.exists(log_file):
    fd = open(log_file, "a")
  else:
    fd = open(log_file, "w")
    fd.write("state timestamp utime stime vsizeMB rssMB\n")
    
  proc = subprocess.Popen(args, close_fds=True)
  adjust_omm_score(proc.pid)
  
  def _timeout(x, y): raise Timeout()
  handler = signal.signal(signal.SIGALRM, _timeout)

  ret = None
  while ret == None:
    try:
      signal.alarm(delay_between_checks)
      ret = proc.wait()
      signal.alarm(0)
      break
    except Timeout:
      write_stats(fd, proc.pid)

  signal.signal(signal.SIGALRM, handler)
  fd.write("s(%s) %s NA NA NA NA\n" % (ret, int(time.time())))
  fd.close()
  
  return ret

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "Usage: logfile delay_in_seconds args..."
    sys.exit(1)

  log_file = sys.argv[1]
  delay = int(sys.argv[2])
  ret = run_and_watch(log_file, sys.argv[3:], delay_between_checks=delay)
  sys.exit(ret)
