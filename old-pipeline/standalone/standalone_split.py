import os, datetime
from mstransform import mstransform

dt = datetime.datetime.now()
timestr = dt.strftime('%Y-%m-%dT%H-%M-%S')

ms_dir = '/mnt/Data/data1/20131122_941_6_FINAL_PRODUCTS/13B-266.sb27261805.eb28549602.56618.334173599535_calibrated_deepfield.ms'

output_vis = '/mnt/output/Chiles/split_vis/{0}'.format(timestr)

os.makedirs(output_vis)
gap = 4

start_time = datetime.datetime.now()
for i in range(1):
    mstransform(vis=ms_dir,
                outputvis='{0}/vis_{1}~{2}'.format(output_vis, 1020 + i * gap, 1024 + i * gap),
                start='{0}MHz'.format(1020 + i * gap),
                width='15.625kHz',
                spw='2~2',
                nchan=256,
                regridms=True,
                restfreq='1420.405752MHz',
                mode='frequency',
                outframe='lsrk',
                interpolation='linear',
                veltype='radio',
                combinespws=True,
                nspw=1,
                createmms=False,
                datacolumn="data")

end_time = datetime.datetime.now()
print 'Time taken:', end_time, start_time, end_time - start_time
