#!/usr/bin/python 
# -*- coding: utf-8 -*-

import os, sys
import datetime
import time

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

PYTHON = "/usr/bin/python"
YAS3FS = "/home/ewah/git/yas3fs-ewah/yas3fs/__init__.py"

AWS_ACCESS_KEY_ID='AKIAIIBZRWEPEQVLNVIQ'
AWS_SECRET_ACCESS_KEY='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'
AWS_TOPIC='arn:aws:sns:us-west-2:757791176761:ewah-s3fs'
AWS_REGION='us-west-2'

base = {
	's3_bucket' : 's3.140507',
	's3_path' : '/t',
	'local_path' : 'mnt',
	'cache_path' : '/home/ewah/yas3fs-test/cache',
	'log_path' : 'logs'
}

run_date = datetime.datetime.now()
run_id = run_date.strftime("%Y%m%d")

base_options = [
		'-recheck-s3',
		'--region', AWS_REGION,
		'--topic', AWS_TOPIC,
		'--nonempty'
		'--s3-num', '2'
		'--with-plugin-class RecoverYas3fsPlugin'
		'--new-queue-with-hostname',
		'--cache-on-disk', '0'
		]

mount = {}

# for point in ['a', 'b', 'c', 'd']:
for point in ['a', 'b']:
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

	mount[point]['command'] = " ".join([
			PYTHON,
			YAS3FS,
			"--debug",
			"s3://" +  mount[point]['s3_bucket'] + mount[point]['s3_path'],
			mount[point]['local_path'],
			"--recheck-s3", 
			"--region", AWS_REGION,
			"--cache-path" , mount[point]['cache_path'], 
			"--nonempty", 
			"--s3-num=5", 
			"--with-plugin-class", "RecoverYas3fsPlugin", 
			"--topic", AWS_TOPIC,
			"--new-queue-with-hostname",
			"--aws-managed-encryption", 
			'--cache-on-disk', '0',
			"--cache-disk-size 5000", 
			"--mp-size 100 --mp-num 4 --mp-retries 3 --st-blksize 131072 --read-retries-num 10 --read-retries-sleep 1 --download-retries-num 20 --download-retries-sleep 5",
			"--log", mount[point]['log_path'] + "/yas4fs.log", 
			"--log-backup-gzip",

			"> /dev/null"])

	mount[point]['env'] = {
		'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
		'AWS_SECRET_ACCESS_KEY':AWS_SECRET_ACCESS_KEY
	}

