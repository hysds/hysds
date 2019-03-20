import os
import boto3, botocore
import requests, urllib3
import json
import re
from datetime import datetime
import logging as logger
import csv
import argparse

from util import pull_mozart_errors_es, process_head_object_errors, check_dataset_exists_grq, check_file_in_s3_bucket, \
	mozart_purge_job

urllib3.disable_warnings()

if not os.path.exists(os.getcwd() + "/logs"):
	os.mkdir(os.getcwd() + "/logs")


# SETTING UP THE LOGGER AND CSV, SETTING UP ARGUMENT PARSER
################################################################################
LOG_FILE_NAME = os.getcwd() + "/logs/" + "purge_no_head_obj_{}.log"
logger.basicConfig(format='%(asctime)s %(message)s',
					filename=LOG_FILE_NAME.format(datetime.now()),
					filemode='w',
					level=logger.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--force", help="Prod Run - Purge jobs", action="store_true")
prod_purge = parser.parse_args()
if prod_purge.force:
	print("PROD PURGE TURNED ON, WILL PURGE JOBS")
	logger.info("#"*100 + "\nPROD PURGE TURNED ON, WILL PURGE JOBS\n" + "#"*100)
else:
	print("prod purge turned off, will not purge jobs")
	logger.info("#"*100 + "\nPROD PURGE TURNED OFF, WILL NOT PURGE JOBS\n" + "#"*100)

CSV_FILE_NAME = "logs/slc_purge_results_{}.csv".format(datetime.now())
slc_results_file = open(CSV_FILE_NAME, 'w')
csv_writer = csv.writer(slc_results_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
csv_writer.writerow(["bucket", "key", "result", "webpage"])


# HARD CODED VARIABLES AND SETTING UP THE CSV FILE
################################################################################
ERROR = "Failed to download s.....ot Found\n\n"
MOZART_ES_URL = "https://c-mozart.aria.hysds.io/es/_search"
GRQ_ES_URL = "https://c-datasets.aria.hysds.io/es/_search"

s3 = boto3.resource('s3')
BUCKETS = ["aria-ops-dataset-bucket", "hysds-aria" ]
S3_BASE_URL = "s3://s3-us-west-2.amazonaws.com:80/"
################################################################################

logger.info("PULLING ERRORS FROM GRQ ELASTICSEARCH")
missing_head_obj_errors = pull_mozart_errors_es(MOZART_ES_URL, ERROR, logger)
logger.info("Pulled {} errors: '{}' in Mozart".format(len(missing_head_obj_errors), ERROR))

logger.info("PARSING SLC AND S3 FILE PATH FROM TRACEBACK")
slc_regex = ".*(S1.+).zip.*"
slcs = process_head_object_errors(missing_head_obj_errors, slc_regex, logger)

logger.info("CHECKING FOR SLC IN GRQ AND S3")

counter = 1
for slc in slcs:
	logger.info("Counter: {}".format(counter))
	in_s3 = True
	s3_url, hijacked_url = check_dataset_exists_grq(GRQ_ES_URL, slc, logger)

	if s3_url:  # GRQ RECORD EXISTS AND ALSO HAS A S3 URL
		# CHECKING IF FILE EXISTS IN S3, IF SO, DO NOTHING, IF NOT, PURGE
		s3_path_split = s3_url.replace(S3_BASE_URL, '').split('/')
		bucket = s3_path_split[0]
		key = '/'.join(s3_path_split[1:]) + '/' + slc + '.zip'
		print(bucket, key)
		in_s3 = check_file_in_s3_bucket(s3, bucket, key, hijacked_url, logger, csv_writer)
	else:  # GRQ RECORD DOES NOT EXIST OR DOES NOT HAVE A S3 URL
		logger.info("WILL NOT PURGE {}".format(slc))
		logger.info("")
		continue

	if not in_s3:
		if prod_purge.force:  # PRODUCTION MODE, FLAG -f ADDED
			logger.info("NOT FOUND IN S3 AND WILL PURGE {}".format(slc))
			mozart_purge_job(slc, logger)  # purge function for mozart
			# break
		else:  # DEV MODE, FLAG -f NOT ADDED
			logger.info("NOT FOUND IN S3 AND WILL NOT PURGE {}".format(slc))
			logger.info("Put -f flag to purge in production")
	logger.info("")
	counter += 1
