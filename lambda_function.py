import json

import boto3

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
sns_arn = "arn:aws:sns:us-east-1:403526594903:s3-dataprocessed-notification"


def lambda_handler(event, context):
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        lines = response["Body"].read().decode("utf-8").split("\n")
        op = []
        for line in lines:
            item = json.loads(line)
            if item.get("status") == "delivered":
                op.append(line)
        op_str = "\n".join(op)

        print("writing to s3")
        s3_destination_key = s3_file_key[0:11] + "input.json"
        destination_bucket = "doordash-target-zn12"
        s3_client.put_object(
            Bucket=destination_bucket,
            Key=s3_destination_key,
            Body=op_str.encode("utf-8"),
        )
        sub = "Successfull: Data processed"
        message = "Input s3 file {} / {} has been processed succesfully".format(
            bucket_name, s3_destination_key
        )
        sns_client.publish(
            Subject=sub, TargetArn=sns_arn, Message=message, MessageStructure="text"
        )
    except Exception as err:
        print(err)
        sub = "Failed: data processing failed"
        message = "input s3 file {} / {} has been failed Error = {}".format(
            bucket_name, s3_destination_key, err
        )
        sns_client.publish(
            Subject=sub, TargetArn=sns_arn, Message=message, MessageStructure="text"
        )
