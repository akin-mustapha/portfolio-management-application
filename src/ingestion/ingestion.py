import json
import base64
import logging
import time

from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


API_URL = "https://live.trading212.com/api/v0/"

secret_name = "prod/t212"
region_name = "eu-west-1"


_ENDPOINTS = {
    "account": {
        "endpoint": "equity/account/summary",
        "bucket_name": "t212-asset",
        "key": "/account/bronze/"
    },
    "positions": {
        "endpoint": "equity/positions",
        "bucket_name": "t212-asset",
        "key": "/positions/bronze-positions/"
    }
    # "dividends": "equity/history/dividends",
    # "orders": "equity/history/orders",
    # "transactions": "equity/history/transactions",
}

session = boto3.session.Session()


def get_secret():
    try:
        client = session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        logger.error(
            "[SECRETS] Failed to retrieve credentials: %s",
            e
        )
        raise

    secret = json.loads(
        get_secret_value_response["SecretString"]
    )

    return (
        secret.get("T212_API_TOKEN"),
        secret.get("T212_SECRET_TOKEN")
    )

def fetch_enpoint(
  url: str,
  header,
) -> tuple[list[dict], float]:
  """Retrieve all open equity positions."""

  start = time.perf_counter()
  try:
    request = Request(
        url,
        headers=header,
        method="GET"
    )

    with urlopen(request, timeout=10) as response:
      payload = response.read().decode("utf-8")
      result = json.loads(payload)

    result = (
      result
      if isinstance(result, list)
      else result.get("items", [])
    )

    duration = time.perf_counter() - start

    logger.info(
      "[API] Positions retrieved successfully | "
      "records=%d | duration=%.2fs",
      len(result),
      duration
    )

    return result, duration

  except Exception as e:
    duration = time.perf_counter() - start
    logger.error(
      "[API] Failed to retrieve data from %s | "
      "duration=%.2fs | error=%s",
      url,
      duration,
      e
    )

    raise

def save_to_s3(
  data: list[dict],
  bucket_name: str,
  key: str
) -> float:
  """Save data to S3."""

  start = time.perf_counter()
  count_records = len(data)

  try:
    client = session.client(
      service_name="s3",
      region_name=region_name,
    )

    now = datetime.now(timezone.utc)

    data = [
      {
        **pos,
        "ingested_timestamp": str(now),
        "ingested_date": now.isoformat(),
      }
      for pos in data
    ]

    body = "\n".join(
      json.dumps(record)
      for record in data
    )

    logger.info(
      "[S3] Uploading data | "
      "records=%d | destination=s3://%s/%s",
      count_records,
      bucket_name,
      key
    )

    client.put_object(
      Bucket=bucket_name,
      Key=key,
      Body=body,
      ContentType="application/json"
    )

    duration = time.perf_counter() - start

    logger.info(
      "[S3] Positions written successfully | "
      "records=%d | duration=%.2fs",
      count_records,
      duration
    )
    return duration

  except ClientError as e:
    duration = time.perf_counter() - start

    logger.error(
      "[S3] Upload failed | "
      "records=%d | duration=%.2fs | error=%s",
      count_records,
      duration,
      e
    )

    # Dead Letter
    client.put_object(
      Bucket=bucket_name,
      Key=f"dead-letters/{key}",
      Body=body,
      ContentType="application/json"
    )

    logger.error(
      "[S3] Data written to dead-letter location | "
      "destination=s3://%s/dead-letters/%s",
      bucket_name,
      key
    )
    raise
    
    
def lambda_handler(event, context):
  logger.info("=" * 60)
  logger.info("Trading212 Pipeline Execution")
  logger.info("=" * 60)

  pipeline_start = time.perf_counter()

  # ---------------------------------------------------------
  # Secrets
  # ---------------------------------------------------------

  logger.info("[SECRETS] Retrieving API credentials")

  API_TOKEN, SECRET_TOKEN = get_secret()

  logger.info("[SECRETS] Credentials retrieved successfully")

  # ---------------------------------------------------------
  # Trading212 API
  # ---------------------------------------------------------

  credentials = f"{API_TOKEN}:{SECRET_TOKEN}"

  token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

  header = {"Authorization": f"Basic {token}"}
  
  for k, v in _ENDPOINTS.items():
    
    logger.info("")
    
    endpoint = v.get("endpoint")
    
    bucket_name = v.get("bucket_name")
    
    key = v.get("")
    
    url = f"{API_URL}/{endpoint}"
  
    res, api_duration = fetch_enpoint(url, header)

    record_count = len(res)

    # ---------------------------------------------------------
    # S3
    # ---------------------------------------------------------

    now = datetime.now(timezone.utc)

    key = (
      f"{v.get("")}"
      f"{now.year}/{now.month:02d}/{now.day:02d}/"
      f"{k}_"
      f"{now.isoformat()}.json"
    )

    s3_duration = save_to_s3(
      res,
      bucket_name,
      key
    )

  # ---------------------------------------------------------
  # Execution Summary
  # ---------------------------------------------------------

  total_duration = time.perf_counter() - pipeline_start

  logger.info("")
  logger.info("-" * 60)
  logger.info("Execution Summary")
  logger.info("-" * 60)

  logger.info(
    "API request                  %4d records    %.2fs",
    record_count,
    api_duration
  )

  logger.info(
    "S3 upload                    %4d records    %.2fs",
    record_count,
    s3_duration
  )

  logger.info("-" * 60)

  logger.info(
    "Total                        %4d records    %.2fs",
    record_count,
    total_duration
  )

  logger.info("=" * 60)
  logger.info("Trading212 Pipeline Completed Successfully")
  logger.info("=" * 60)

  return {
    "statusCode": 200,
    "records": record_count,
    "duration_seconds": round(total_duration, 2)
  }
