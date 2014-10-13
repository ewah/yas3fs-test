import os, sys
from nose.tools import *
from subprocess import *

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import time
import logging
import boto
import settings
import json

'''
run with nosetest -v 
'''

def test_ok():
	''' just checking'''

def test_unmount_all():
	for point in settings.mount:
		p = Popen("umount " +  settings.mount[point]['local_path'], shell=True, env=settings.mount[point]['env'])

	time.sleep(10)
