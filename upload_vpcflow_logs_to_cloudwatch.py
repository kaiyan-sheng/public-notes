import boto3
from datetime import datetime
import os


def create_log_event(log_group, log_stream, events, region):
    # Initialize the CloudWatchLogs client
    cloudwatch_logs = boto3.client('logs',
                                   region,
                                   aws_access_key_id=access_key_id,
                                   aws_secret_access_key=secret_access_key,
                                   aws_session_token=session_token)

    try:
        # Put log event to CloudWatch Logs
        cloudwatch_logs.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=events
        )
    except Exception as e:
        print("Error creating log event:", e)


# Configs
# Set AWS credentials
access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
session_token = os.environ.get('AWS_SESSION_TOKEN')
log_group_name = '/aws/kinesisfirehose/firehose-test-ks'

# run #1 with vpcflow logs
log_stream_name = 'lg1'
region_name = 'us-east-1'
number_of_log_events = 500000
batch_size = 5000

# # run #2 with vpcflow logs
# log_stream_name = 'lg2'
# region_name = 'us-east-1'
# number_of_log_events = 1000000
# batch_size = 5000

# Generate log events
log_events = []
for i in range(number_of_log_events):
    # Create a VPCFlow log event with a unique message and timestamp
    log_event = {
        'timestamp': int(datetime.now().timestamp() * 1000),
        'message': f'{i+1} 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK'
    }
    log_events.append(log_event)

    # Sending batches of <batch_size> log events at a time
    if len(log_events) == batch_size:
        create_log_event(log_group_name, log_stream_name, log_events, region_name)
        log_events = []
