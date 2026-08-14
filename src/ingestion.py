import json
import base64
import logging
from typing import cast
from urllib.parse import urljoin
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


API_URL = "https://live.trading212.com/api/v0/"
secret_name = "prod/t212"
region_name = "eu-west-1"
bucket_name = "t212-asset"
    
    
session = boto3.session.Session()



def lambda_handler(event, context):
    logger.info("Starting lambda")
    
    logger.info("Getting secrets")
    API_TOKEN, SECRET_TOKEN = get_secret()

    logger.info("Getting positions")
    positions = get_positions(API_URL, API_TOKEN, SECRET_TOKEN)
    
    now = datetime.now(timezone.utc)
    key = f"positions/{now.year}/{now.month:02d}/{now.day:02d}/positions_{now.isoformat()}.json"
    

    try:
        logger.info("Connecting to s3")

        client = session.client(
            service_name="s3",
            region_name=region_name,
        )
        logger.info("Connected to s3")
        logger.info(f"s3://{bucket_name}/{key}")

        positions = [
            {
                **pos,
                "ingested_date": now.isoformat(),
            }
            for pos in positions
        ]
        body = "\n".join(json.dumps(record) for record in positions)

        logger.info("Pushing to s3")
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json"

        )
    except ClientError as e:
        logger.error(f"Error occurred while pushing to s3: {e}")
        raise e

    return {"statusCode": 200}


def get_secret():
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    return secret.get("T212_API_TOKEN"), secret.get("T212_SECRET_TOKEN")


def get_positions(url: str, api_token: str, secret_token: str) -> list[dict]:
    """Return all open equity positions (synchronous)."""
    
    credentials = f"{api_token}:{secret_token}"
    token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    
    header = {"Authorization": f"Basic {token}"}
    url = urljoin(url, "equity/positions")

    try:
      request = Request(url, headers=header, method="GET")
      with urlopen(request, timeout=10) as response:
          payload = response.read().decode("utf-8")
          result = json.loads(payload)
      
    except Exception as e:
        logger.error(f"Error occurred while fetching positions: {e}")
        raise e

    return result if isinstance(result, list) else result.get("items", [])