import boto3
from datetime import datetime
import json
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

# # run #3 with cloudtrail logs
log_stream_name = 'lg3_cloudtrail'
region_name = 'us-east-1'
number_of_log_events = 1000
batch_size = 500

# # run #4 with cloudtrail logs
# log_stream_name = 'lg4_cloudtrail'
# region_name = 'us-east-1'
# number_of_log_events = 100000
# batch_size = 500

# Generate log events
log_events = []
for i in range(number_of_log_events):
    # Create CloudTrail log event
    sample_record = {
        "eventVersion": "1.09",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": "AIDAZEDJODE3DG5YNDC7L",
            "arn": "arn:aws:iam::123456789012:user/elastic-agent-test",
            "accountId": "123456789012",
            "accessKeyId": "AKIAZEDJODE3LAMJDVVW",
            "userName": "elastic-agent-david-hope"
        },
        "eventTime": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "eventSource": "ec2.amazonaws.com",
        "eventName": "DescribeInstances",
        "awsRegion": "ap-northeast-1",
        "sourceIPAddress": "35.175.197.10",
        "userAgent": "aws-sdk-go-v2/1.18.0 os/linux lang/go/1.20.12 md/GOOS/linux md/GOARCH/arm64 api/ec2/1.36.1",
        "requestParameters": {
            "instancesSet": {},
            "filterSet": {}
        },
        "responseElements": {
            "instancesSet": {
                "items": [{"instanceId": "i-1234567890abcdef0"}]
            }
        },
        "requestID": "85c50261-e596-4322-81b4-41594c7859ca",
        "eventID": f"{i}",
        "readOnly": True,
        "eventType": "AwsApiCall",
        "managementEvent": True,
        "recipientAccountId": "123456789012",
        "eventCategory": "Management",
        "tlsDetails": {
            "tlsVersion": "TLSv1.3",
            "cipherSuite": "TLS_AES_128_GCM_SHA256",
            "clientProvidedHostHeader": "ec2.ap-northeast-1.amazonaws.com"
        }
    }
    log_event = {
        'timestamp': int(datetime.now().timestamp() * 1000),
        'message': json.dumps(sample_record)
    }
    log_events.append(log_event)

    # Sending batches of <batch_size> log events at a time
    if len(log_events) == batch_size:
        create_log_event(log_group_name, log_stream_name, log_events, region_name)
        log_events = []
