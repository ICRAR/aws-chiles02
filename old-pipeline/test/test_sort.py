"""

"""
from os.path import basename


list = [
    '/mnt/input/Chiles/cube_1000~1004.image',
    '/mnt/input/Chiles/cube_1004~1008.image',
    '/mnt/input/Chiles/cube_1008~1012.image',
    '/mnt/input/Chiles/cube_1012~1016.image',
    '/mnt/input/Chiles/cube_1016~1020.image',
    '/mnt/input/Chiles/cube_1020~1024.image',
    '/mnt/input/Chiles/cube_1024~1028.image',
    '/mnt/input/Chiles/cube_1028~1032.image',
    '/mnt/input/Chiles/cube_1032~1036.image',
    '/mnt/input/Chiles/cube_988~992.image',
    '/mnt/input/Chiles/cube_992~996.image',
    '/mnt/input/Chiles/cube_996~1000.image'
]


def fixed_frequency(cube_name):
    base_name = basename(cube_name)
    elements = base_name.split('.')
    elements = elements[0].split('_')
    elements = elements[1].split('~')
    return int(elements[0])


new_list = sorted(list, key=fixed_frequency)

print new_list
