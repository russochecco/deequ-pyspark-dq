import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.profiles import ColumnProfilerRunner

APP_NAME = "DeequPySparkDQ"
CSV_SOURCE_PATH = os.environ["CSV_SOURCE_PATH"]
PARQUET_OUTPUT_PATH = os.environ["PARQUET_OUTPUT_PATH"]

os.environ["SPARK_VERSION"] = "3.5"

SCHEMA = StructType([
    StructField("VIN", StringType(), True),
    StructField("County", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("ModelYear", IntegerType(), True),  # Fixed to IntegerType
    StructField("Make", StringType(), True),
    StructField("ElectricVehicleType", StringType(), True),
    StructField("ElectricRange", IntegerType(), True)
])

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)s {APP_NAME}: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S"
)

logger = logging.getLogger(APP_NAME)

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sparkContext.logLevel", "WARN")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("delimiter", "\t")
        .option("header", True)
        .csv(CSV_SOURCE_PATH, schema=SCHEMA)
    )

    # Step 1: Data Profiling
    profile_result = (
        ColumnProfilerRunner(spark)
        .onData(df)
        .run()
    )

    for col, profile in profile_result.profiles.items():
        logger.info(f"Column: {col} | Completeness: {profile.completeness} | DataType: {profile.dataType}")

    # Step 2: Define constraints and verify data
    check = (
        Check(spark, CheckLevel.Error, APP_NAME)
        .satisfies("ElectricRange >= 0 and ElectricRange < 400", "Valid ElectricRange value")
        .isUnique("VIN")
        .isComplete("City")
        .isComplete("State")
        .isComplete("County")
    )

    verification_result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run()
    )

    # Step 3: Process the results
    check_results_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)

    (
        check_results_df.select("check", "constraint", "constraint_status", "constraint_message")
        .show(truncate=False)
    )

    # Step 4: Conditional routing based on pipeline status
    if verification_result.status == "Success":
        logger.info("Data quality checks passed! Writing data to partitioned Parquet layer...")

        (
            df.repartition("ModelYear", "Make")
            .write.mode("overwrite")
            .partitionBy("ModelYear", "Make")
            .parquet(PARQUET_OUTPUT_PATH)
        )
    else:
        logger.warning("Data quality checks failed! Routing data to quarantine zone.")

        (
            check_results_df.filter(check_results_df.constraint_status != "Success")
            .show(truncate=False)
        )

    logger.info("Stopping Spark Application.")
    spark.stop()
