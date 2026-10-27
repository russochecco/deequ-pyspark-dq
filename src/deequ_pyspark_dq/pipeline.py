import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.profiles import ColumnProfilerRunner

APP_NAME = "DeequPySparkDQ"
log = logging.getLogger(APP_NAME)

# Enforce Spark compatibility configuration before runtime context initialization
os.environ["SPARK_VERSION"] = "4.1"

# Define immutable target schema specifications
SCHEMA = StructType([
    StructField("VIN", StringType(), True),
    StructField("County", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("ModelYear", IntegerType(), True),
    StructField("Make", StringType(), True),
    StructField("ElectricVehicleType", StringType(), True),
    StructField("ElectricRange", IntegerType(), True)
])


def setup_logging():
    """Configures systemic pipeline logging diagnostics."""
    if not log.hasHandlers():
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s %(name)s: %(message)s',
            datefmt="%y/%m/%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        log.addHandler(console_handler)

    log.setLevel(logging.INFO)


def get_spark_session() -> SparkSession:
    """Creates or retrieves a standard execution context Spark Session."""
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sparkContext.logLevel", "WARN")
        .getOrCreate()
    )


def run_pipeline(spark: SparkSession, config: dict):
    """Executes dataset profile assessment and validation suites before writing partitions."""
    log.info("Initializing Data Quality Assurance Verification Pipeline...")

    log.info("Ingesting target dataset from source location...")
    df = (
        spark.read
        .option("delimiter", "\t")
        .option("header", True)
        .csv(config["CSV_SOURCE_PATH"], schema=SCHEMA)
    )

    log.info("Executing comprehensive data profile runner...")
    profile_result = ColumnProfilerRunner(spark).onData(df).run()

    for col, profile in profile_result.profiles.items():
        log.info(f"Column: {col} | Completeness: {profile.completeness} | DataType: {profile.dataType}")

    log.info("Configuring PyDeequ quality check thresholds...")
    check = (
        Check(spark, CheckLevel.Error, APP_NAME)
        .satisfies("ElectricRange >= 0 and ElectricRange < 400", "Valid ElectricRange value")
        .isUnique("VIN")
        .isComplete("City")
        .isComplete("State")
        .isComplete("County")
    )

    log.info("Running constraint verification suite on dataset...")
    verification_result = VerificationSuite(spark).onData(df).addCheck(check).run()

    # Conditional routing based on data validation outcomes
    if verification_result.status == "Success":
        log.info("Data quality checks successfully passed! Committing to Parquet sink...")
        (
            df.repartition("ModelYear", "Make")
            .write.mode("overwrite")
            .partitionBy("ModelYear", "Make")
            .parquet(config["PARQUET_OUTPUT_PATH"])
        )
    else:
        log.error("Data quality assertions failed constraint verification rules!")

        # Pull failure breakdown structural matrix for localized visibility
        check_results_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)

        (
            check_results_df.select("check", "constraint", "constraint_status", "constraint_message")
            .filter(check_results_df.constraint_status != "Success")
            .show(truncate=False)
        )

        # Raise an exception to halt the execution framework and handle exit protocols safely
        raise ValueError("Pipeline stopped: Dataset does not satisfy target data quality checks.")


if __name__ == "__main__":
    setup_logging()

    # Consolidate environmental requirements cleanly before setting up engine
    pipeline_config = {
        "CSV_SOURCE_PATH": os.environ["CSV_SOURCE_PATH"],
        "PARQUET_OUTPUT_PATH": os.environ["PARQUET_OUTPUT_PATH"]
    }

    session = get_spark_session()

    try:
        run_pipeline(session, pipeline_config)
    except Exception as e:
        log.exception("Data quality verification pipeline halted with error:")
        sys.exit(1)
    finally:
        log.info("Stopping Spark Application Session Context safely.")
        session.stop()
