import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import DoubleType, LongType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ResolveChoice
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


logger = glueContext.get_logger()

INPUT_PATH = "s3://t212-asset/positions/"

dyf = glueContext.create_dynamic_frame.from_options(
        connection_type = "s3",
        connection_options={
            "paths": [INPUT_PATH], "recurse": True
        },
        format_options={"multiLine": "true"},
        format="json"
    )
logger.info("Schema")
dyf.printSchema()

logger.info(f"DynamicFrame count = {dyf.count()}")
dyf = dyf.resolveChoice(specs=[
    ("averagePricePaid", "cast:double"),
    ("currentPrice", "cast:double"),
    ("quantity", "cast:double"),
    ("quantityAvailableForTrading", "cast:long"),
    ("quantityInPies", "cast:double"),
    ("walletImpact.currentValue", "cast:double"),
    ("walletImpact.totalCost", "cast:double"),
    ("walletImpact.unrealizedProfitLoss", "cast:double")
])

df = dyf.toDF()

flattened_df = (
    df.select(
        col("instrument.name").alias("instrument_name"),
        col("instrument.ticker").alias("instrument_ticker"),
        col("instrument.isin").alias("instrument_isin"),
        to_timestamp(col("createdAt")).alias("created_at"),
        col("instrument.currency").alias("currency"),
        col("averagePricePaid").cast(DoubleType()).alias("average_price_paid"),
        col("currentPrice").cast(DoubleType()).alias("current_price"),
        col("quantity").cast(DoubleType()).alias("quantity"),
        col("quantityAvailableForTrading")
            .cast(DoubleType())
            .alias("quantity_available_for_trading"),
        col("quantityInPies")
            .cast(DoubleType())
            .alias("quantity_in_pies"),
        col("walletImpact.currency").alias("wallet_impact_currency"),
        col("walletImpact.currentValue")
            .cast(DoubleType())
            .alias("wallet_impact_current_value"),
        col("walletImpact.fxImpact")
            .alias("wallet_impact_fx_impact"),
        col("walletImpact.totalCost")
            .cast(DoubleType())
            .alias("wallet_impact_total_cost"),
        col("walletImpact.unrealizedProfitLoss")
            .cast(DoubleType())
            .alias("wallet_impact_unrealized_profit_loss"),
    
        to_date(col("ingested_date")).alias("ingested_date")
    )
)

logger.info(f"DataFrame count = {df.count()}")

logger.info("Writing to S3")
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(flattened_df, glueContext, "positions_dynamic_frame"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://t212-asset/silver-positions/",
        "partitionKeys": ["ingested_date"]
        
    })
job.commit()