"""

"""
import cPickle as pickle


def convert_cpu_stats_to_num_array(cpuStats):
    """
    Given a list of statistics (tuples[timestamp, total_cpu, kernel_cpu, vm, rss])
    Return five numarrays
    """
    print "Converting cpus stats into numpy array"
    c0 = []
    c1 = []
    c2 = []
    c3 = []
    c4 = []
    # TODO - need a pythonic/numpy way for corner turning
    gc.disable()
    for c in cpuStats:
        c0.append(c[0])
        c1.append(c[1])
        c2.append(c[2])
        c3.append(c[3])
        c4.append(c[4])
    gc.enable()

    return (np.array(c0), np.array(c1), np.array(c2), np.array(c3), np.array(c4))


def plot_cpu_mem_usage_from_file(cpufile, figfile, stt=None, x_max=None, time_label=None):
    """
    Plot CPU and memory usage from a cpu log file

    parameters:
      cpufile:  the full path of the cpu log file (string)
      figfile:  the full path of the plot file (string)
      stt:  start time stamp in seconds (Integer,
                                         None if let it done automatically)
      x_max: the duration of the time axis in seconds (Integer,
                                                       None automatically set)
      time_label: full path to the application activity log (string)
                  each line is something like this:
                  2014-08-17 04:44:24 major cycle 3
                  2014-08-17 04:45:44 make image

                  If set, the plot tries to draw vertical lines along the
                  time axis to show these activities This is an experimental
                  feature, need more work

    """
    reList = []
    if os.path.exists(cpufile):
        try:
            pkl_file = open(cpufile, 'rb')
            print 'Loading CPU stats object from file %s' % cpufile
            cpuStatsList = pickle.load(pkl_file)
            pkl_file.close()
            if cpuStatsList == None:
                raise Exception("The CPU stats object is None when reading from the file")
            reList += cpuStatsList
            #return cpuStatsList

        except Exception, e:
            ex = str(e)
            import traceback
            print 'Fail to load the CPU stats from file %s: %s' % (cpufile, ex)
            traceback.print_exc()
            raise e
    else:
        print 'Cannot locate the CPU stats file %s' % cpufile
    fig = pl.figure()
    plot_cpu_mem_usage(fig, x_max, reList, stt, standalone = True, time_label = time_label)
    #fig.savefig('/tmp/cpu_mem_usage.pdf')
    fig.savefig(figfile)
    pl.close(fig)


def plot_cpu_mem_usage(fig, cpuStats, x_max = None, stt = None,
                    standalone = False, time_label = None):
    if standalone:
        ax1 = fig.add_subplot(111)
    else:
        ax1 = fig.add_subplot(211)
    ax1.set_xlabel('Time (seconds)', fontsize = 9)

    ax1.set_ylabel('CPU usage (% of Wall Clock time)', fontsize = 9)
    ax1.set_title('CPU and Memory usage', fontsize=10)
    ax1.tick_params(axis='both', which='major', labelsize=8)
    ax1.tick_params(axis='both', which='minor', labelsize=6)

    # get the data in numpy array
    ta, tc, kc, vm, rss = convert_cpu_stats_to_num_array(cpuStats)
    if stt is None:
        stt = ta
    ta -= stt
    st = int(ta[0])
    ed = int(ta[-1])
    if x_max is None:
        x_max = ed
    elif ed > x_max:
        x_max = ed

    # create x-axis (whole integer seconds) between st and ed
    # x = np.r_[st:ed + 1]
    x = ta.astype(np.int64)

    # plot the total cpu
    ax1.plot(x, tc, color = 'g', linestyle = '-', label = 'total cpu')

    # plot the kernel cpu
    ax1.plot(x, kc, color = 'r', linestyle = '--', label = 'kernel cpu')

    # plot the virtual mem

    ax2 = ax1.twinx()
    ax2.set_ylabel('Memory usage (MB)', fontsize = 9)
    ax2.tick_params(axis='y', which='major', labelsize=8)
    ax2.tick_params(axis='y', which='minor', labelsize=6)
    ax2.plot(x, vm / 1024.0 ** 2, color = 'b', linestyle = ':', label = 'virtual memory')

    # plot the rss
    ax2.plot(x, rss / 1024.0 ** 2, color = 'k', linestyle = '-.', label = 'resident memory')
    mmm = max(tc)
    ax1.set_ylim([0, 1.5 * mmm])

    ax1.set_xlim([0, x_max]) # align the time axis to accommodate cpu/memory

    # it should read a template and then populate the time
    if time_label:
        import datetime
        with open(time_label) as f:
            c = 0
            for line in f:
                fs = line.split('\t')
                aa = fs[0].replace(' ', ',').replace('-',',').replace(':',',')
                aaa = aa.split(',')
                tstamp = (datetime.datetime(int(aaa[0]),int(aaa[1]),int(aaa[2]),int(aaa[3]),int(aaa[4]),int(aaa[5])) - datetime.datetime(1970,1,1)).total_seconds()
                tstamp -= stt
                if (c % 2 == 0):
                    delt = 0
                    co = 'k'
                    ls = 'dotted'
                else:
                    delt = 50
                    co = 'm'
                    ls = 'dashed'
                ax1.vlines(tstamp, 0, 1.5 * mmm, colors = co, linestyles=ls)
                ax1.text(tstamp - 25, 1 * mmm + delt, fs[1], fontsize = 7)
                c += 1
    ax1.legend(loc='upper left', shadow=True, prop={'size':8})
    ax2.legend(loc='upper right', shadow=True, prop={'size':8})
