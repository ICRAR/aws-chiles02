"""

"""

import os
from os.path import join

from imstat import imstat

from makecube_defines import cube_dir, freq_min, freq_max

print '''cube_dir = {0}'''.format(cube_dir)

clean_number = int(os.getenv('CLEAN_NUMBER', -1))
cube_dir = join(cube_dir, '{0}'.format(clean_number))

output_image = join(cube_dir, 'cube_{0}~{1}'.format(freq_min, freq_max))
print '''output_image = {0}'''.format(output_image)


imstat(imagename=output_image+'.image')
imstat(imagename=output_image+'.residual')
