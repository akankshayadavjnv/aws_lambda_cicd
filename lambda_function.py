import boto3
import pandas as pd

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
sns_arn = "arn:aws:sns:us-east-1:403526594903:s3-dataprocessed-notification"


def lambda_handler(event, context):
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        res = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        data = pd.read_json(res["Body"], lines=True)
        print("data", data)
        filter_data = data[data["status"] == "delivered"]
        print("filter_data", filter_data)
        destination_bucket = "doordash-target-zn12"

        buffer = filter_data.to_json(
            lines=True,
            orient="records",
        )
        print("buffer", buffer)
        print("writing to s3")
        s3_client.put_object(Bucket=destination_bucket, Key=s3_file_key, Body=buffer)
        sub = "Successfull: data processing"
        message = "input s3 file {} {} has been processed succesfully".format(
            bucket_name, s3_file_key
        )
        sns_client.publish(
            Subject=sub, TargetArn=sns_arn, Message=message, MessageStructure="text"
        )
    except Exception as err:
        print(err)
        sub = "Failed: data processing failed"
        message = "input s3 file {} {} has been failed Error = {}".format(
            bucket_name, s3_file_key, err
        )
        sns_client.publish(
            Subject=sub, TargetArn=sns_arn, Message=message, MessageStructure="text"
        )
