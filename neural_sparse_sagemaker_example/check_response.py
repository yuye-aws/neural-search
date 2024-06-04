import boto3

body = ["hello world"]
client = boto3.client('sagemaker-runtime')

client.
