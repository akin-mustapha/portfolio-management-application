"""
Glue Python Shell job: Trading212 positions, silver -> gold.

Enriches silver positions with asset metadata (sector, industry, asset
type) from a static mapping file, and writes the result to the gold
zone. Incremental with watermarking + explicit backfill, matching the
bronze -> silver job's conventions.

Unlike bronze (plain year/month/day folders, not cataloged), silver is
already Hive-style partitioned (ingested_date=YYYY-MM-DD/) since the
bronze -> silver job writes it via awswrangler's partition_cols. Gold
follows the same Hive-style convention.

Job setup (Python Shell, not Spark):
- Python version: 3.9
- Job parameters:
    --JOB_NAME                   financial-dataflow-silver-to-gold
    --additional-python-modules  awswrangler==3.*,pandas,pyarrow
- Max capacity: 0.0625 or 1 DPU is plenty at this data volume.
"""
import sys
import json
import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional

import boto3
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
INPUT_PATH = "s3://financial-dataflow/trading212/silver/positions/"   # ingested_date=YYYY-MM-DD/ partitions
OUTPUT_PATH = "s3://financial-dataflow/trading212/gold/positions/"
STATE_PATH = "s3://financial-dataflow/trading212/gold/_state/positions_gold_watermark.json"
MAPPING_BUCKET = "financial-dataflow"
MAPPING_KEY = "resources/asset_mapping.json"
GLUE_DATABASE = "financial_dataflow"
GLUE_TABLE = "gold_positions"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
logger = logging.getLogger(args["JOB_NAME"])
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Optional backfill window, parsed separately from getResolvedOptions
# since getResolvedOptions treats every listed arg as required.
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--START_DATE", default=None, help="YYYY-MM-DD, backfill start (inclusive)")
_parser.add_argument("--END_DATE", default=None, help="YYYY-MM-DD, backfill end (inclusive)")
BACKFILL_ARGS, _ = _parser.parse_known_args(sys.argv[1:])
IS_BACKFILL = bool(BACKFILL_ARGS.START_DATE and BACKFILL_ARGS.END_DATE)


# ---------------------------------------------------------------------
# Watermark helpers (same pattern as bronze -> silver, separate state file)
# ---------------------------------------------------------------------
def get_watermark() -> Optional[date]:
    try:
        state = wr.s3.read_json(STATE_PATH, lines=False)
        return pd.to_datetime(state["last_processed_date"].iloc[0]).date()
    except Exception:
        logger.info("No watermark found at %s — treating this as the first run", STATE_PATH)
        return None


def set_watermark(new_date: date) -> None:
    wr.s3.to_json(df=pd.DataFrame([{"last_processed_date": new_date.isoformat()}]), path=STATE_PATH)
    logger.info("Watermark advanced to %s", new_date)


def dates_to_process() -> List[date]:
    if IS_BACKFILL:
        start = pd.to_datetime(BACKFILL_ARGS.START_DATE).date()
        end = pd.to_datetime(BACKFILL_ARGS.END_DATE).date()
        logger.info("Backfill mode: %s to %s (watermark will NOT be updated)", start, end)
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    watermark = get_watermark()
    today = date.today()
    start = today if watermark is None else watermark + timedelta(days=1)
    if start > today:
        return []
    return [start + timedelta(days=i) for i in range((today - start).days + 1)]


# ---------------------------------------------------------------------
# Read / enrich / write
# ---------------------------------------------------------------------
def read_silver(dates: List[date]) -> pd.DataFrame:
    """Read silver Parquet for the given ingested_date partitions."""
    target_dates = {d.isoformat() for d in dates}
    logger.info("Reading silver partitions for dates: %s", sorted(target_dates))
    try:
        df = wr.s3.read_parquet(
            path=INPUT_PATH,
            dataset=True,
            partition_filter=lambda part: part.get("ingested_date") in target_dates,
        )
    except wr.exceptions.NoFilesFound:
        logger.warning("No silver data found for target dates")
        return pd.DataFrame()
    logger.info("Silver row count for this run: %d", len(df))
    return df


def load_ticker_lookup() -> dict:
    """Load the ticker -> asset metadata mapping from S3."""
    logger.info("Loading asset mapping from s3://%s/%s", MAPPING_BUCKET, MAPPING_KEY)
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=MAPPING_BUCKET, Key=MAPPING_KEY)
    asset_mapping = json.loads(response["Body"].read())

    return {
        v["trading212_ticker"]: {
            "symbol": k,
            "sector": v["sector"],
            "industry": v["industry"],
            "asset_type": v["asset_type"],
        }
        for k, v in asset_mapping["assets"].items()
    }


DEFAULT_MAPPING = {
    "symbol": None,
    "sector": "Unknown",
    "industry": "Unknown",
    "asset_type": "Unknown",
}


def enrich(df: pd.DataFrame, ticker_lookup: dict) -> pd.DataFrame:
    """Join positions to asset metadata by ticker."""
    logger.info("Enriching %d rows with asset mapping", len(df))
    mapping = df["ticker"].map(lambda t: ticker_lookup.get(t, DEFAULT_MAPPING))
    mapping_df = pd.json_normalize(mapping)
    mapping_df.index = df.index

    unmapped = mapping_df["sector"].eq("Unknown").sum()
    if unmapped:
        logger.warning("%d rows had no asset mapping match (defaulted to Unknown)", unmapped)

    return pd.concat([df, mapping_df], axis=1)


def write_gold(df: pd.DataFrame, path: str, database: str, table: str) -> None:
    logger.info("Writing %d rows to %s (partitioned by ingested_date)", len(df), path)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",  # safe to rerun/backfill any date without duplicating
        partition_cols=["ingested_date"],
        database=database,
        table=table,
    )


def main() -> None:
    dates = dates_to_process()
    if not dates:
        logger.info("No new partitions to process. Exiting.")
        return

    df_silver = read_silver(dates)
    if df_silver.empty:
        logger.info("No silver data found for target dates. Exiting without writing.")
        return

    ticker_lookup = load_ticker_lookup()
    df_gold = enrich(df_silver, ticker_lookup)
    write_gold(df_gold, OUTPUT_PATH, GLUE_DATABASE, GLUE_TABLE)

    if not IS_BACKFILL:
        set_watermark(max(dates))

    logger.info("Job complete. Dates processed: %s", [d.isoformat() for d in dates])


if __name__ == "__main__":
    main()