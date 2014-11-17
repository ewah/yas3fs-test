#!/usr/bin/python 
# -*- coding: utf-8 -*-

import os, sys
import datetime
import time
import logging

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

PYTHON = "/usr/bin/python"
YAS3FS = "/usr/bin/yas3fs"
S3CMD = "/usr/bin/s3cmd --server-side-encryption -c /home/ewah/.s3cfg"


AWS_ACCESS_KEY_ID='AAAAAAAAAAAAAAAAAAAA'
AWS_SECRET_ACCESS_KEY='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

AWS_TOPIC='arn:aws:sns:rrrrrr:888888888888:xxxxxx'
AWS_REGION='rrrrrrrrr'

base = {
	's3_bucket' : 'BBBBBBBBBBBBBBBBBBBBBBBBB', # w/o s3://
	's3_path' : '/pppppppppppppp',
	'local_path' : '/tmp/yas3fs-test/mnt',
	'cache_path' : '/tmp/yas3fs-test/cache',
	'log_path' : '/tmp/yas3fs-test/logs'
}

file = {
	'small' : '/tmp/apache.saml.cache',
	'medium' : '/tmp/junk',
	'large' : '/home/ec10176/140127_pre.tar'
}

run_date = datetime.datetime.now()
yymmdd = run_date.strftime("%y%m%d")
hhmiss = run_date.strftime("%H%M%S")
run_id = yymmdd + 'e'

# base wait time before checking if something should show up in boto
boto_wait_time = 1

mount_points = ('a', 'b', 'c')
# A:
# B: identical to A
# C: identical to A except not on the SQS


mount = {}

for point in mount_points:
	mount[point] = {
		's3_path' : base['s3_path'] + '/' + run_id,		# same mount point
		'local_path' : base['local_path'] + '/' + run_id + '/' + point,
		'cache_path' : base['cache_path'] + '/' + run_id + '/' + point,
		'log_path' : base['log_path'] + '/' + run_id + '/' + point,
		's3_bucket': base['s3_bucket'],
		'conn' : S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY),
		'conn2' : S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY),

		'default_headers' : { 'x-amz-server-side-encryption' : 'AES256' }
		}
	mount[point]['conn_bucket'] = mount[point]['conn'].get_bucket(mount[point]['s3_bucket'])
	mount[point]['conn2_bucket'] = mount[point]['conn2'].get_bucket(mount[point]['s3_bucket'])

	mount[point]['s3_fullpath'] = "s3://" +  mount[point]['s3_bucket'] +  mount[point]['s3_path']

	cmd = [
			PYTHON,
			YAS3FS,
			"--debug",
			mount[point]['s3_fullpath'],
			mount[point]['local_path'],
			]

	if len(mount_points) > 1 and point not in ('c'):
		cmd += [
			"--region", AWS_REGION,
			"--topic", AWS_TOPIC,
			"--new-queue-with-hostname",
		]

	if point in ('c'):
		cmd += [
			"--read-only"
		]

	cmd += [
			"--cache-path" , mount[point]['cache_path'], 

			"--recheck-s3", 
			"--nonempty", 
			"--s3-num=5", 
			"--with-plugin-class", "RecoverYas3fsPlugin", 
			"--aws-managed-encryption", 
			'--cache-on-disk', '0',
			"--cache-disk-size 5000", 
			"--mp-size 50", 
			"--mp-num 4 --mp-retries 3 --st-blksize 131072 --read-retries-num 10 --read-retries-sleep 1 --download-retries-num 20 --download-retries-sleep 5",

			"--log", mount[point]['log_path'] + "/yas3fs.log", 
			"--log-backup-gzip",

			"> /dev/null"]

	mount[point]['command'] = " ".join(cmd)
	# print mount[point]['command']

	mount[point]['env'] = {
		'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
		'AWS_SECRET_ACCESS_KEY':AWS_SECRET_ACCESS_KEY
	}

