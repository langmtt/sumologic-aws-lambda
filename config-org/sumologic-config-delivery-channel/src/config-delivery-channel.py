import json
import os
import logging
import sys
import requests
from crhelper import CfnResource
import boto3

# Setup Default Logger
LOGGER = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", logging.DEBUG)
LOGGER.setLevel(log_level)

# Initialize the helper
helper = CfnResource(json_logging=True, log_level=log_level, boto_level="CRITICAL", sleep_on_delete=120)
config_client = boto3.client('config', region_name=os.environ.get("AWS_REGION"))

@helper.create
def create(event, _):
    request_type = event["RequestType"]
    LOGGER.info(f"{request_type} Event")
    params = event.get("ResourceProperties")
    bucket_name = params.get('S3BucketName','')
    bucket_prefix = params.get('s3KeyPrefix','')
    sns_topic_arn  = params.get('snsTopicARN','')
    delivery_frequency = params.get('deliveryFrequency','')
    LOGGER.info(f"DELIVERY CHANNEL - Starting the AWS config Delivery channel create with bucket {bucket_name}")

    name = "default"
    if not bucket_name:
        channels = config_client.describe_delivery_channels()
        if "DeliveryChannels" in channels:
            for channel in channels["DeliveryChannels"]:
                bucket_name = channel["s3BucketName"]
                if not bucket_prefix:
                    bucket_prefix = channel["s3KeyPrefix"] if "s3KeyPrefix" in channel else None
                name = channel["name"]
                break

    delivery_channel = {"name": name, "s3BucketName": bucket_name}
    if bucket_prefix:
        delivery_channel["s3KeyPrefix"] = bucket_prefix
    if sns_topic_arn:
        delivery_channel["snsTopicARN"] = sns_topic_arn
    if delivery_frequency:
        delivery_channel["configSnapshotDeliveryProperties"] = {'deliveryFrequency': delivery_frequency}
    config_client.put_delivery_channel(DeliveryChannel=delivery_channel)
    LOGGER.info("DELIVERY CHANNEL - Completed the AWS config Delivery channel create.")
    return {"DELIVERY_CHANNEL": "Successful"}, name

@helper.update
def update(event, _):
    return create(event)

@helper.delete
def delete(event, _):
    params = event.get("ResourceProperties")
    remove_on_delete_stack = (params.get("RemoveOnDeleteStack", "false")).lower() in "true"
    bucket_name = params.get('S3BucketName','')
    if remove_on_delete_stack:
        if not bucket_name:
            create(event)
        else:
            config_client.delete_delivery_channel(DeliveryChannelName="default")
        LOGGER.info("DELIVERY CHANNEL - Completed the AWS Config delivery channel delete.")
    else:
        LOGGER.info("DELIVERY CHANNEL - Skipping the AWS Config delivery channel delete.")

def lambda_handler(event, context):
    LOGGER.info(f"Invoking AWS Config source {event['source']} region {event['region']}")
    """Lambda Handler.
    Args:
        event: event data
        context: runtime information
    Raises:
        ValueError: Unexpected error executing Lambda function
    """
    LOGGER.info("....Lambda Handler Started....")
    try:
        helper(event, context)
    except Exception as error:
        LOGGER.error(f"Unexpected Error: {error}")
        raise ValueError(f"Unexpected error executing Lambda function. Review CloudWatch logs '{context.log_group_name}' for details.") from None
