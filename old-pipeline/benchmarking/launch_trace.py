import subprocess, os, sys
import calendar, time


def usage():
    print 'python launch_trace.py app'
    print 'e.g. python launch_trace.py ls -l'

def trace():
    cmdlist = sys.argv[1:]
    start_time = calendar.timegm(time.gmtime())
    #print "CPU recording start_time: ", start_time
    cpu_logfile = '%s_cpu.log' % str(start_time)
    app_logfile = '%s_app.log' % str(start_time)
    h_app_logfile = open(app_logfile, 'w')

    sp = subprocess.Popen(cmdlist, stdout=h_app_logfile)
    cmd1 = 'python2.7 trace_cpu_mem.py -o %s -p %d' % (cpu_logfile, sp.pid)
    cmdlist1 = cmd1.split()
    sp1 = subprocess.Popen(cmdlist1)

    print "Waiting...."
    print "Application return code:", sp.wait()

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        usage()
        sys.exit(1)
    trace()
