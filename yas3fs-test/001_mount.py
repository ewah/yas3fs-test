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

def test_prep_local_dir():
	for point in settings.mount:
		for path in ['local_path', 'log_path', 'cache_path']:
			# TODO USE os.mkdirs or equivalent
			p = Popen("mkdir -p " + settings.mount[point][path], shell=True)
			p.communicate()

			# assert exists
			assert_equals(os.path.exists(settings.mount[point][path]), True)

			# assert root owns paths
			path_stat = os.stat(settings.mount[point][path])
			assert_equals(path_stat.st_uid, 0)

def test_prep_s3():
	for point in settings.mount:
		# creates root directory and check that i pulls up. 
		k = Key(settings.mount[point]['conn_bucket'])
		k.key = settings.mount[point]['s3_path'] + "/"

		headers = { 'Content-Type': 'application/x-directory'}
		headers.update(settings.mount[point]['default_headers'])
		k.metadata['yas3fs_mount_test'] = "HELLO? " + point
		k.set_contents_from_string('', headers)

		# second connection gets a attr
		k2 = Key(settings.mount[point]['conn2_bucket'])

		k2 = Key(settings.mount[point]['conn2_bucket'])
		logging.error(k2)


		assert_equals(k.metadata['yas3fs_mount_test'], "HELLO? " + point)


def test_mount_all():
	for point in settings.mount:
		logging.error(settings.mount[point]['command'])

		p = Popen(settings.mount[point]['command'], shell=True, env=settings.mount[point]['env'], stdout=None, stderr=None)
		p.communicate()

	for point in settings.mount:
		found = 0
		for tries in (1,2,3):
			p = Popen('mount | grep ' + settings.mount[point]['local_path'], shell = True, stdout=PIPE)
			mount_rt = p.communicate()

			if mount_rt[0]:
				found = 1
				break
			time.sleep(1)

		assert_equals(found, 1)


