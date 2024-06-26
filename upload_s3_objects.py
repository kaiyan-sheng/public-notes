import json
import boto3
import gzip
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor


def create_log_event(sample_log, bucket_name, file_name, region):
    json_log = json.dumps(sample_log)
    # Convert string to bytes
    json_bytes = json_log.encode('utf-8')
    gzip_object = gzip.compress(json_bytes)

    # Initialize the s3 client (ensure it's thread-safe)
    s3_client = boto3.client('s3',
                             region_name=region,
                             aws_access_key_id=access_key_id,
                             aws_secret_access_key=secret_access_key,
                             aws_session_token=session_token)
    try:
        # Put log event to S3 bucket
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=gzip_object, ContentType='application/json', ContentEncoding='gzip')
        print(f"Uploaded sample log to s3://{bucket_name}/{file_name}")
    except Exception as e:
        print("Error creating log event:", e)


# Configs
access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
session_token = os.environ.get('AWS_SESSION_TOKEN')

number_of_log_events = 5000
number_of_samples = 100000
region_name = 'us-east-1'
bucket_name = "test-s3-sqs-ks-5-folders"
number_of_workers = 50
print(f"Start ingesting logs. Current timestamp is: {datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}")


# Generate log events
def generate_and_upload_logs(j):
    sample_records = []
    for i in range(number_of_samples):
        sample_record = {
            "eventVersion": "1.09",
            "userIdentity": {
                "type": "IAMUser",
                "principalId": "AIDAZEDJODE3DG5YNDC7L",
                "arn": "arn:aws:iam::123456789012:user/elastic-agent-david-hope",
                "accountId": "123456789012",
                "accessKeyId": "AKIAZEDJODE3LAMJDVVW",
                "userName": "elastic-agent-david-hope"
            },
            # "eventTime": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
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
            "eventID": f"{j}-{i}",
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
        sample_records.append(sample_record)

    sample_log = {"Records": sample_records}
    # Number of sub folders
    number_of_sub_folders = 5

    # Generate file names
    file_names = [
        f"cloudflaretest/cloudflare-name-test-{i+1}/{datetime.today().strftime('%Y%m%d')}/sample-log-{j}-{datetime.now().timestamp()}.gz"
        for i in range(number_of_sub_folders)
    ]

    for file_name in file_names:
        create_log_event(sample_log, bucket_name, file_name, region_name)


# Upload logs using multiple threads
with ThreadPoolExecutor(max_workers=number_of_workers) as executor:
    futures = [executor.submit(generate_and_upload_logs, j) for j in range(number_of_log_events)]

# Wait for all threads to complete
for future in futures:
    future.result()

print(f"All logs uploaded successfully. Current timestamp is: {datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}")
