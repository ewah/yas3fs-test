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

def test_write_empty_file_a():
	# writes an empty file to mount point 'a'

	fname = "/test_write_empty_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	local_b_file =  settings.mount['b']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("touch " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 0)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 0)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 0)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)
		

def test_write_20byte_file_a():
	# writes an empty file to mount point 'a'

	fname = "/test_write_20byte_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	local_b_file =  settings.mount['b']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo -n '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 20)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)

	# takes 1 second to catch up?!
	time.sleep(1)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 20)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 20)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)

def test_chown_1000_1000_file_a():
	# writes an empty file to mount point 'a'

	fname = "/test_chown_1000_1000_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	local_b_file =  settings.mount['b']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	p = Popen("chown 1000.1000 " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 21)
	assert_equals(local_stat.st_uid, 1000)
	assert_equals(local_stat.st_gid, 1000)

	# takes 1 second to catch up?!
	time.sleep(1)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 21)
	assert_equals(s3_stat['st_uid'], 1000)
	assert_equals(s3_stat['st_gid'], 1000)

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 1000)
	assert_equals(local_b_stat.st_gid, 1000)

def test_utime_1_file_a():
	# writes an empty file to mount point 'a'

	fname = "/test_utime_1_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	local_b_file =  settings.mount['b']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	time.sleep(1)

	os.utime(local_file, (1,1))

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 21)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)
	assert_equals(local_stat.st_atime, 1)
	assert_equals(local_stat.st_mtime, 1)
#	assert_equals(local_stat.st_ctime, 1)

	# takes 1 second to catch up?!
	time.sleep(1)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 21)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	#yas3fs does not update atime
#	assert_equals(s3_stat['st_atime'], 1)

	assert_equals(s3_stat['st_mtime'], 1)
#	assert_equals(s3_stat['st_ctime'], 1)

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)

	#yas3fs does not update atime
#	assert_equals(local_b_stat.st_atime, 1)
	assert_equals(local_b_stat.st_mtime, 1)
#	assert_equals(local_b_stat.st_ctime, 1)

