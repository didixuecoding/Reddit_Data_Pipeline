import boto3
import configparser
import logging.config
from pathlib import Path

# Setting up logger, logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")  # get the path of logging.ini file
logger = logging.getLogger(__name__)  # setup logger

# Loading configuration from s3_buckets.cfg
config = configparser.ConfigParser()
config.read_file(open('s3_buckets.cfg'))


def create_buckets(s3_client):
    """
    Create multiple s3 buckets according to s3_buckets.cfg
    :param s3_client: an S3 service client instance
    :return: True if required s3 buckets were created successfully.
    """
    # Get buckets names per config file
    landing_bucket = config.get("S3", "LANDING_ZONE")
    working_bucket = config.get("S3", "WORKING_ZONE")
    processed_bucket = config.get("S3", "PROCESSED_ZONE")
    location = config.get("S3", "LOCATION")

    # Create landing bucket
    try:
        if str(location) == 'us-east-1':
            create_landing_response = s3_client.create_bucket(
                Bucket=str(landing_bucket),
            )
        else:
            create_landing_response = s3_client.create_bucket(
                Bucket=str(landing_bucket),
                CreateBucketConfiguration={
                    'LocationConstraint': str(location)
                },
            )
        logger.debug(f"Got response from s3 client for creating bucket: {create_landing_response}")
        logger.info(f"Bucket create response code : {create_landing_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while creating {landing_bucket} bucket : {e}")
        return False

    # Create working bucket
    try:
        if str(location) == 'us-east-1':
            create_working_response = s3_client.create_bucket(
                Bucket=str(working_bucket),
            )
        else:
            create_working_response = s3_client.create_bucket(
                Bucket=str(working_bucket),
                CreateBucketConfiguration={
                    'LocationConstraint': str(location)
                },
            )
        logger.debug(f"Got response from s3 client for creating bucket: {create_working_response}")
        logger.info(f"Bucket create response code : {create_working_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while creating {working_bucket} bucket : {e}")
        return False

    # Create processed bucket
    try:
        if str(location) == 'us-east-1':
            create_processed_response = s3_client.create_bucket(
                Bucket=str(processed_bucket),
            )
        else:
            create_processed_response = s3_client.create_bucket(
                Bucket=str(processed_bucket),
                CreateBucketConfiguration={
                    'LocationConstraint': str(location)
                },
            )
        logger.debug(f"Got response from s3 client for creating bucket: {create_processed_response}")
        logger.info(f"Bucket create response code : {create_processed_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while creating {processed_bucket} bucket : {e}")
        return False

    return True if (create_landing_response['ResponseMetadata']['HTTPStatusCode'] ==
                    create_working_response['ResponseMetadata']['HTTPStatusCode'] ==
                    create_processed_response['ResponseMetadata']['HTTPStatusCode'] == 200) else False


def delete_buckets(s3_client):
    """
    Delete multiple s3 buckets according to s3_buckets.cfg
    :param s3_client: an S3 service client instance
    :return: True if required s3 buckets were deleted successfully.
    """
    # Get buckets names per config file
    landing_bucket = config.get("S3", "LANDING_ZONE")
    working_bucket = config.get("S3", "WORKING_ZONE")
    processed_bucket = config.get("S3", "PROCESSED_ZONE")

    # Delete landing bucket
    try:
        delete_landing_response = s3_client.delete_bucket(
            Bucket=str(landing_bucket),
        )
        logger.debug(f"Got response from s3 client for deleting bucket: {delete_landing_response}")
        logger.info(f"Bucket delete response code : {delete_landing_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while deleting {landing_bucket} bucket : {e}")
        return False

    # Delete working bucket
    try:
        delete_working_response = s3_client.delete_bucket(
            Bucket=str(working_bucket),
        )
        logger.debug(f"Got response from s3 client for deleting bucket: {delete_working_response}")
        logger.info(f"Bucket delete response code : {delete_working_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while deleting {working_bucket} bucket : {e}")
        return False

    # Delete processed bucket
    try:
        delete_processed_response = s3_client.delete_bucket(
            Bucket=str(processed_bucket),
        )
        logger.debug(f"Got response from s3 client for deleting bucket: {delete_processed_response}")
        logger.info(f"Bucket delete response code : {delete_processed_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occurred while deleting {processed_bucket} bucket : {e}")
        return False

    return True if (delete_landing_response['ResponseMetadata']['HTTPStatusCode'] ==
                    delete_working_response['ResponseMetadata']['HTTPStatusCode'] ==
                    delete_processed_response['ResponseMetadata']['HTTPStatusCode'] == 204) else False


if __name__ == "__main__":
    # Creating low-level service clients
    session = boto3.Session(profile_name='default')
    s3_client = session.client('s3', region_name=str(config.get("S3", "LOCATION")))
    logger.info("Clients setup for S3 services.")

    if create_buckets(s3_client):
        logger.info("All required S3 buckets have been created...")
    else:
        logger.error("Fail to create required S3 bucket...")
