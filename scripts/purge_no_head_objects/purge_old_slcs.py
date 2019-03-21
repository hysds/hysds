import os
import boto3, botocore
import requests, urllib3
import json
import re
from datetime import datetime
import logging as logger
import csv
import argparse

from pprint import pprint
from util import pull_old_slcs, check_file_in_s3_bucket, mozart_purge_job

urllib3.disable_warnings()

if not os.path.exists(os.getcwd() + "/logs"):
    os.mkdir(os.getcwd() + "/logs")

s3 = boto3.resource('s3')

################################################################################
# SETTING UP THE LOGGER AND CSV, SETTING UP ARGUMENT PARSER
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--force", help="Prod Run - Purge jobs", action="store_true")
prod_purge = parser.parse_args()
if prod_purge.force:
    print("PROD PURGE TURNED ON, WILL PURGE JOBS\n")
else:
    print("prod purge turned off, will not purge jobs\n")


PROD_OR_TEST = "prod" if prod_purge.force else "test"
LOG_FILE_NAME = os.getcwd() + "/logs/" + "purge_old_slcs_{}_{}.log".format(datetime.now(), PROD_OR_TEST)
print("log file location: " + LOG_FILE_NAME)
logger.basicConfig(format='%(asctime)s %(message)s',
                   filename=LOG_FILE_NAME,
                   filemode='w',
                   level=logger.INFO)

CSV_FILE_NAME = "logs/purge_old_slcs_{}_{}.csv".format(datetime.now(), PROD_OR_TEST)
print("CSV file location: " + CSV_FILE_NAME)
slc_results_file = open(CSV_FILE_NAME, 'w')
csv_writer = csv.writer(slc_results_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
csv_writer.writerow(["bucket", "key", "result", "webpage"])
################################################################################


GRQ_ES_URL = "https://c-datasets.aria.hysds.io/es/_search"
S3_BASE_URL = "s3://s3-us-west-2.amazonaws.com:80/"
BATCH_SIZE = 500
OLD_BUCKETS = ["hysds-aria"]

pagination = 0
counter = 1
while True:
    slcs = pull_old_slcs(GRQ_ES_URL, pagination + 1, BATCH_SIZE)
    pagination += BATCH_SIZE

    for slc in slcs:
        slc_id = slc["_id"]
        s3_url = slc["urls"][1]
        hijacked_url = slc["urls"][0]
        s3_path_split = s3_url.replace(S3_BASE_URL, '').split('/')
        bucket = s3_path_split[0]
        key = '/'.join(s3_path_split[1:])

        if bucket in OLD_BUCKETS:
            logger.info("counter: {}\tbucket: {}\tSLC: {}".format(counter, bucket, slc_id))
            key = key + "/" + slc_id + ".zip"
            slc_in_s3 = check_file_in_s3_bucket(s3, bucket, key)
            if not slc_in_s3:
                if prod_purge.force:
                    mozart_purge_job(slc_id, logger)
                    logger.info("NOT FOUND IN S3 SENT TO PURGE: {}".format(slc_id))
                else:
                    logger.info("NOT FOUND IN S3 AND WILL NOT PURGE {}".format(slc_id))
                    logger.info("Put -f flag to purge in production")
                csv_writer.writerow([bucket, key, "NOT FOUND IN S3, WILL PURGE", hijacked_url])
            else:
                logger.info("found in S3: {}/{}".format(bucket, key))
                csv_writer.writerow([bucket, key, "found in S3, will not purge", hijacked_url])

        if counter % 100 == 0:
            print("{} files processed".format(counter))
        counter += 1

    if len(slcs) == 0:
        break

print(counter)
