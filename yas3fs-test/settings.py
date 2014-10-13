#!/usr/bin/python 

import os, sys
import datetime
import time

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

AWS_ACCESS_KEY_ID='XXXXXXXXXXXXXXXXXXXX'
AWS_SECRET_ACCESS_KEY='XXXXXXXXXXXXXXXXXXXX'

base = {
	's3_bucket' : 's3.140507',
	's3_path' : '/yas3fs_test',
	'local_path' : 'mnt',
	'cache_path' : 'cache',
	'log_path' : 'logs'
}

run_id = '141013'

base_options = [
		'-recheck-s3',
		'--region', 'us-west-2',
		'--topic', 'arn:aws:sns:us-west-2:757791176761:ewah-s3fs',
		'--nonempty'
		'--s3-num', '2'
		'--with-plugin-class RecoverYas3fsPlugin'
		'--new-queue-with-hostname'
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
			"/usr/bin/python /usr/bin/yas3fs --debug",
			"s3://" +  mount[point]['s3_bucket'] + mount[point]['s3_path'],
			mount[point]['local_path'],
			"--recheck-s3 --region us-west-2",
			"--cache-path " + mount[point]['cache_path'], 
			"--nonempty", 
			"--s3-num=5", 
			" --with-plugin-class RecoverYas3fsPlugin", 
			"--topic arn:aws:sns:us-west-2:757791176761:ewah-s3fs", 
			"--new-queue-with-hostname --aws-managed-encryption", 
			"--log " + mount[point]['log_path'] + "/yas4fs.log", 
			"--log-backup-gzip",

			"> /dev/null"])

	mount[point]['env'] = {
		'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
		'AWS_SECRET_ACCESS_KEY':AWS_SECRET_ACCESS_KEY
	}

# mount point a
# /usr/bin/python /usr/bin/yas3fs s3://s3.140507/public/test z --recheck-s3 --region us-west-2 --cache-path /tmp/yas3fs_z --nonempty --s3-num=20 --with-plugin-class RecoverYas3fsPlugin --topic arn:aws:sns:us-west-2:757791176761:ewah-s3fs --new-queue-with-hostname --aws-managed-encryption --log z_ss.log --log-backup-gzip

