import boto3
import json

def get_aws_secret(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId= secret_name)["SecretString"]
    return json.loads(response)