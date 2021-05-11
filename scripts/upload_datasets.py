import os
import sys
import threading
import glob

import boto3
import logging.config
import configparser
from pathlib import Path

# Setting up logger, logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")  # get the path of logging.ini file
logger = logging.getLogger(__name__)  # setup logger

# Loading configuration from s3_buckets.cfg
config = configparser.ConfigParser()
config.read_file(open('s3_buckets.cfg'))


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def upload_file(s3_client, file_name, object_name=None):
    """Upload a file to an given S3 bucket
    :param s3_client: an S3 service client instance
    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # read bucket name from cfg file
    bucket = config.get('S3', 'LANDING_ZONE')

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name.split('\\')[-1]

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, Callback=ProgressPercentage(file_name))
#        logger.debug(f"Got response from s3 client for uploading file: {response}")
    except Exception as e:
        logger.error(f"Error occurred while upload {file_name} : {e}")
        return False
    return True


def get_all_files(filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


if __name__ == "__main__":
    # Creating low-level service clients
    session = boto3.Session(profile_name='default')
    s3_client = session.client('s3', region_name=str(config.get("S3", "LOCATION")))
    logger.info("Clients setup for S3 services.")
    # List all files needed to be uploaded
    filepath = f"{Path(__file__).parents[0].parents[0]}/input"
    all_files = get_all_files(filepath=filepath)
    # Upload each file
    success = fail = 0
    for i, file in enumerate(all_files):
        if upload_file(s3_client=s3_client, file_name=file):
            success += 1
            logger.info(f"{file} has been successfully uploaded...")
            logger.info(f"{success}/{len(all_files)} completed...")
        else:
            fail += 1
            logger.error(f"{file} failed to be uploaded...")
            logger.info(f"{fail} files failed...")

