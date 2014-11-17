#!/usr/bin/python
# -*- coding: utf-8 -*-

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


def __get_base_dir():
  return "/test_recheck_a_00/"

def __get_file_prefix():
  return "test_new_"

def test_ok():
	''' just checking'''

def test_make_directory_a():
	fname = __get_base_dir()
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("mkdir " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	assert_equals(os.path.exists(local_file), True)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 0)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)
	# logging.error(k.metadata)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(os.path.exists(local_b_file), True)

def test_recheck_s3cmd_c():
	# writes an empty file to mount point 'a'

	if len(settings.mount_points) <= 2:
		return

	fname = __get_base_dir() + __get_file_prefix()  + "test_recheck_s3cmd.txt" + settings.hhmiss
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	local_c_file =  settings.mount['c']['local_path'] + fname
	s3_c_file =  settings.mount['c']['s3_path'] + fname
	s3_c_fullfile =  settings.mount['c']['s3_fullpath'] + fname

	if (os.path.exists(local_c_file)):
		p = Popen("rm " + local_c_file, shell=True)
		p.communicate()
	assert_equals(os.path.exists(local_c_file), False)

	p = Popen("stat " + local_c_file, shell=True)
	p.communicate()

	src_stat = os.stat(settings.file['small'])

	p = Popen(settings.S3CMD + " put " + settings.file['small'] + " " + s3_c_fullfile, shell=True)
	p.communicate()

	# takes 1 second to catch up?!
	# time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	assert_not_equals(k, None)
	# no attr yet.
	# s3_stat = json.loads(k.metadata['attr'])
	# assert_equals(s3_stat['st_size'], src_stat.st_size)
	assert_equals(bool('attr' in k.metadata), False)

	assert_equals(os.path.exists(local_c_file), True)
	# can i access it on the other node? (even though it doesnt go by sns)
	local_c_stat = os.stat(local_c_file)
	assert_equals(local_c_stat.st_size, src_stat.st_size)
	assert_equals(local_c_stat.st_uid, 0)
	assert_equals(local_c_stat.st_gid, 0)


def test_recheck_c():
	# writes an empty file to mount point 'a'

	if len(settings.mount_points) <= 2:
		return

	fname = __get_base_dir() + __get_file_prefix()  + "test_recheck_a.txt" + settings.hhmiss
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	local_b_file =  settings.mount['b']['local_path'] + fname
	local_c_file =  settings.mount['c']['local_path'] + fname

	if (os.path.exists(local_file)):
		logging.error("rm " + local_file)
		p = Popen("rm " + local_file, shell=True)
		p.communicate()
		time.sleep(2)
	assert_equals(os.path.exists(local_file), False)

	if (os.path.exists(local_b_file)):
		p = Popen("rm " + local_b_file, shell=True)
		p.communicate()
	assert_equals(os.path.exists(local_b_file), False)

	if (os.path.exists(local_c_file)):
		p = Popen("rm " + local_c_file, shell=True)
		p.communicate()
	assert_equals(os.path.exists(local_c_file), False)

	p = Popen("echo ABC > " + local_file, shell=True)
	p.communicate()

	p = Popen("chown 1000.1000 " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 4)
	assert_equals(local_stat.st_uid, 1000)
	assert_equals(local_stat.st_gid, 1000)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
#	s3_stat = json.loads(k.metadata['attr'])
#	assert_equals(s3_stat['st_size'], 4)
#	assert_equals(s3_stat['st_uid'], 1000)
#	assert_equals(s3_stat['st_gid'], 1000)

	# can i access it on the other node?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 4)
	assert_equals(local_b_stat.st_uid, 1000)
	assert_equals(local_b_stat.st_gid, 1000)

	# can i access it on the other node? (even though it doesnt go by sns)
	local_c_stat = os.stat(local_c_file)
	assert_equals(local_c_stat.st_size, 4)
	assert_equals(local_c_stat.st_uid, 1000)
	assert_equals(local_c_stat.st_gid, 1000)
