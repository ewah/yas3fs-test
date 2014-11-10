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
  return "/test£_new_dir_a_01/"

def __get_file_prefix():
  return "test£_new_"

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

def test_make_subdirectory_a():
	fname = __get_base_dir() + __get_file_prefix()  + "subdirectory/"
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


def test_write_empty_file_a():
	# writes an empty file to mount point 'a'

	fname = __get_base_dir() + __get_file_prefix()  + "empty_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("touch " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 0)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)

	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 0)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 0)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)
		

def test_write_20byte_file_a():
	# writes an empty file to mount point 'a'

	fname = __get_base_dir() + __get_file_prefix()  + "write_20byte_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo -n '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 20)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 20)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 20)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)

def test_chown_1000_1000_file_a():
	fname = __get_base_dir() + __get_file_prefix()  + "chown_1000_1000_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	if k:
		settings.mount['a']['conn_bucket'].delete_key(k)

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
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 21)
	assert_equals(s3_stat['st_uid'], 1000)
	assert_equals(s3_stat['st_gid'], 1000)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 1000)
	assert_equals(local_b_stat.st_gid, 1000)

def test_utime_1_file_a():
	fname = __get_base_dir() + __get_file_prefix()  + "utime_1_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

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
	time.sleep(settings.boto_wait_time)

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

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)

	#yas3fs does not update atime
#	assert_equals(local_b_stat.st_atime, 1)
	assert_equals(local_b_stat.st_mtime, 1)
#	assert_equals(local_b_stat.st_ctime, 1)

def test_chmod_000_file_a():
	fname = __get_base_dir() + __get_file_prefix()  + "chmod_000_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	os.chmod(local_file, 0000)

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 21)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)
	assert_equals(local_stat.st_mode, 32768)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 21)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)
	assert_equals(s3_stat['st_mode'], 32768)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)
	assert_equals(local_b_stat.st_mode, 32768)

def test_chmod_644_file_a():
	fname = __get_base_dir() + __get_file_prefix()  + "chmod_644_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	os.chmod(local_file, 0644)

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 21)
	assert_equals(local_stat.st_uid, 0)
	assert_equals(local_stat.st_gid, 0)
	assert_equals(local_stat.st_mode, 33188)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 21)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)
	assert_equals(s3_stat['st_mode'], 33188)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, 21)
	assert_equals(local_b_stat.st_uid, 0)
	assert_equals(local_b_stat.st_gid, 0)
	assert_equals(local_b_stat.st_mode, 33188)


def test_create_via_cp_large_a():
	src_fname = settings.file['large']

	fname = __get_base_dir() + __get_file_prefix()  + "cp_file_large_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("cp " +  src_fname + " " + local_file, shell=True)
	p.communicate()

	src_stat = os.stat(src_fname)

	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, src_stat.st_size)

	# takes 10 second to catch up?!
	time.sleep(10)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(k.size, src_stat.st_size)
	assert_equals(s3_stat['st_size'], src_stat.st_size)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, src_stat.st_size)


def test_create_via_cp_a():
	src_fname = settings.file['small']

	fname = __get_base_dir() + __get_file_prefix()  + "cp_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("cp " +  src_fname + " " + local_file, shell=True)
	p.communicate()

	src_stat = os.stat(src_fname)

	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, src_stat.st_size)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(k.size, src_stat.st_size)
	assert_equals(s3_stat['st_size'], src_stat.st_size)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, src_stat.st_size)


def test_create_sym_link_a():
	src_fname = settings.file['small']

	fname = __get_base_dir() + __get_file_prefix()  + "sym_link_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("ln -s " +  src_fname + " " + local_file, shell=True)
	p.communicate()

	src_stat = os.stat(src_fname)

	# can i access it locally?
	local_lstat = os.lstat(local_file)

	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, src_stat.st_size)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(k.size, len(src_fname))
	assert_equals(s3_stat['st_size'], 0)
	assert_equals(s3_stat['st_uid'], 0)
	assert_equals(s3_stat['st_gid'], 0)

	if len(settings.mount_points) <=1:
		return 

	local_b_file =  settings.mount['b']['local_path'] + fname

	# can the other mount see it?
	local_b_stat = os.stat(local_b_file)
	assert_equals(local_b_stat.st_size, src_stat.st_size)

def test_rm_file_a():
	# writes an empty file to mount point 'a'

	fname = __get_base_dir() + __get_file_prefix()  + "move_source_rm_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	p = Popen("echo -n '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	p = Popen("chown 1000.1000 " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 20)
	assert_equals(local_stat.st_uid, 1000)
	assert_equals(local_stat.st_gid, 1000)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 20)
	assert_equals(s3_stat['st_uid'], 1000)
	assert_equals(s3_stat['st_gid'], 1000)

	if len(settings.mount_points) > 1:
		local_b_file =  settings.mount['b']['local_path'] + fname

		# can the other mount see it?
		local_b_stat = os.stat(local_b_file)
		assert_equals(local_b_stat.st_size, 20)
		assert_equals(local_b_stat.st_uid, 1000)
		assert_equals(local_b_stat.st_gid, 1000)

	p = Popen("rm " + local_file, shell=True)
	p.communicate()

	# sleep of 0.1 yields about 50% returns
	# time.sleep(.1)
	# sleep of 0.25 yields is at 100%
	# time.sleep(.25)

	assert_equals(os.path.isfile(local_file), False)

	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	assert_equals(k, None)


def test_move_file_a():
	# writes an empty file to mount point 'a'

	fname = __get_base_dir() + __get_file_prefix()  + "move_source_file_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	target_fname = __get_base_dir() + __get_file_prefix()  + "move_target_file_a.txt"
	target_local_file =  settings.mount['a']['local_path'] + target_fname
	target_s3_file =  settings.mount['a']['s3_path'] + target_fname

	p = Popen("echo -n '12345678901234567890' >  " + local_file, shell=True)
	p.communicate()

	p = Popen("chown 1000.1000 " + local_file, shell=True)
	p.communicate()

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, 20)
	assert_equals(local_stat.st_uid, 1000)
	assert_equals(local_stat.st_gid, 1000)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], 20)
	assert_equals(s3_stat['st_uid'], 1000)
	assert_equals(s3_stat['st_gid'], 1000)

	if len(settings.mount_points) > 1:
		local_b_file =  settings.mount['b']['local_path'] + fname

		# can the other mount see it?
		local_b_stat = os.stat(local_b_file)
		assert_equals(local_b_stat.st_size, 20)
		assert_equals(local_b_stat.st_uid, 1000)
		assert_equals(local_b_stat.st_gid, 1000)

	p = Popen("mv " + local_file + " " + target_local_file, shell=True)
	p.communicate()

	assert_equals(os.path.isfile(local_file), False)
	assert_equals(os.path.isfile(target_local_file), True)
	target_local_stat = os.stat(target_local_file)
	assert_equals(target_local_stat.st_size, 20)
	assert_equals(target_local_stat.st_uid, 1000)
	assert_equals(target_local_stat.st_gid, 1000)

	time.sleep(settings.boto_wait_time)

	# what does boto say?
	target_k = settings.mount['a']['conn_bucket'].get_key(target_s3_file)
	target_s3_stat = json.loads(target_k.metadata['attr'])
	assert_equals(target_s3_stat['st_size'], 20)
	assert_equals(target_s3_stat['st_uid'], 1000)
	assert_equals(target_s3_stat['st_gid'], 1000)

	if len(settings.mount_points) > 1:
		assert_equals(os.path.isfile(local_b_file), False)

		target_local_b_file =  settings.mount['b']['local_path'] + target_fname

		# can the other mount see it?
		target_local_b_stat = os.stat(target_local_b_file)
		assert_equals(target_local_b_stat.st_size, 20)
		assert_equals(target_local_b_stat.st_uid, 1000)
		assert_equals(target_local_b_stat.st_gid, 1000)

def test_move_file_then_append_a():
	# writes an empty file to mount point 'a'

	fname = __get_base_dir() + __get_file_prefix()  + "move_source_file_then_append_a.txt"
	local_file =  settings.mount['a']['local_path'] + fname
	s3_file =  settings.mount['a']['s3_path'] + fname

	target_fname = __get_base_dir() + __get_file_prefix()  + "move_target_file_then_append_a.txt"
	target_local_file =  settings.mount['a']['local_path'] + target_fname
	target_s3_file =  settings.mount['a']['s3_path'] + target_fname

	p = Popen("cat " + settings.file['medium'] + " > " + local_file, shell=True)
	p.communicate()

	p = Popen("chown 1000.1000 " + local_file, shell=True)
	p.communicate()

	src_stat = os.stat(settings.file['medium'])

	# can i access it locally?
	local_stat = os.stat(local_file)
	assert_equals(local_stat.st_size, src_stat.st_size)
	assert_equals(local_stat.st_uid, 1000)
	assert_equals(local_stat.st_gid, 1000)

	# takes 1 second to catch up?!
	time.sleep(settings.boto_wait_time)

	# what does boto say?
	k = settings.mount['a']['conn_bucket'].get_key(s3_file)
	s3_stat = json.loads(k.metadata['attr'])
	assert_equals(s3_stat['st_size'], src_stat.st_size)
	assert_equals(s3_stat['st_uid'], 1000)
	assert_equals(s3_stat['st_gid'], 1000)

	if len(settings.mount_points) > 1:
		local_b_file =  settings.mount['b']['local_path'] + fname

		# can the other mount see it?
		local_b_stat = os.stat(local_b_file)
		assert_equals(local_b_stat.st_size, src_stat.st_size)
		assert_equals(local_b_stat.st_uid, 1000)
		assert_equals(local_b_stat.st_gid, 1000)

	p = Popen("mv " + local_file + " " + target_local_file, shell=True)
	p.communicate()

	p = Popen("echo -n 1234567890 >> " + target_local_file, shell=True)
	p.communicate()

	assert_equals(os.path.isfile(local_file), False)
	assert_equals(os.path.isfile(target_local_file), True)
	target_local_stat = os.stat(target_local_file)
	assert_equals(target_local_stat.st_size, src_stat.st_size + 10)
	assert_equals(target_local_stat.st_uid, 1000)
	assert_equals(target_local_stat.st_gid, 1000)

	time.sleep(settings.boto_wait_time)

	# what does boto say?
	target_k = settings.mount['a']['conn_bucket'].get_key(target_s3_file)
	target_s3_stat = json.loads(target_k.metadata['attr'])
	assert_equals(target_s3_stat['st_size'], src_stat.st_size + 10)
	assert_equals(target_s3_stat['st_uid'], 1000)
	assert_equals(target_s3_stat['st_gid'], 1000)

	if len(settings.mount_points) > 1:
		assert_equals(os.path.isfile(local_b_file), False)

		target_local_b_file =  settings.mount['b']['local_path'] + target_fname

		# can the other mount see it?
		target_local_b_stat = os.stat(target_local_b_file)
		assert_equals(target_local_b_stat.st_size, src_stat.st_size + 10)
		assert_equals(target_local_b_stat.st_uid, 1000)
		assert_equals(target_local_b_stat.st_gid, 1000)
