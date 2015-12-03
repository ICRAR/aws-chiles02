"""

"""
import multiprocessing
import io
import itertools
import os
from test_speed import test

# Needs to be a decent sized file
TEST_FILE = '/home/ec2-user/test_file'


def test_io1():
    f = io.open(TEST_FILE, "rb")
    for i in itertools.count():
        record = f.read(16)
        if not record:
            break
    print 'test1: ', i


def test_io2():
    f = open(TEST_FILE, "rb")
    fd = f.fileno()
    for i in itertools.count():
        record = os.read(fd, 16)
        if not record:
            break

    print 'test2: ', i


def test_io3():
    f = open(TEST_FILE, "rb")
    fd = f.fileno()
    offsets = range(0, 1048765, 16)
    i = 0
    while True:
        buff = os.read(fd, 1048576)
        if not buff:
            break

        for offset in offsets:
            record = buff[offset:offset + 16]
            i += 1

    print 'test3: ', i


def test_create_file():
    print 'Creating test file'
    f = open(TEST_FILE, 'w')
    i = 0
    while i < 100000000:
        f.write('0123456789')
        i += 1

if __name__ == '__main__':
    multiprocessing.freeze_support()
    test()
    test_create_file()
    test_io1()
    test_io2()
    test_io3()
