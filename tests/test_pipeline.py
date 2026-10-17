from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

TEST_SCHEMA = StructType([
    StructField("VIN", StringType(), True),
    StructField("County", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("ModelYear", IntegerType(), True),
    StructField("Make", StringType(), True),
    StructField("ElectricVehicleType", StringType(), True),
    StructField("ElectricRange", IntegerType(), True)
])


def test_pipeline_passes_with_clean_data(spark_session):
    """Verifies that flawless records resolve to a 'Success' status state."""
    clean_data = [
        ("1G1RC6S", "King", "Seattle", "WA", 2024, "CHEVROLET", "BEV", 250),
        ("5YJ3E1E", "Pierce", "Tacoma", "WA", 2025, "TESLA", "BEV", 320)
    ]
    df = spark_session.createDataFrame(clean_data, schema=TEST_SCHEMA)

    check = Check(spark_session, CheckLevel.Error, "UnitTesting")
    result = (
        VerificationSuite(spark_session)
        .onData(df)
        .addCheck(
            check.satisfies("ElectricRange >= 0 and ElectricRange < 400", "Valid ElectricRange value")
            .isUnique("VIN")
            .isComplete("City")
        )
        .run()
    )

    assert result.status == "Success"


def test_pipeline_fails_with_invalid_electric_range(spark_session):
    """Verifies that an out-of-bounds electric range triggers a verification failure."""
    corrupt_data = [
        ("1G1RC6S", "King", "Seattle", "WA", 2024, "CHEVROLET", "BEV", 450)  # Invalid: 450 >= 400
    ]
    df = spark_session.createDataFrame(corrupt_data, schema=TEST_SCHEMA)

    check = Check(spark_session, CheckLevel.Error, "UnitTesting")
    result = (
        VerificationSuite(spark_session)
        .onData(df)
        .addCheck(check.satisfies("ElectricRange >= 0 and ElectricRange < 400", "Valid ElectricRange value"))
        .run()
    )

    assert result.status == "Error"


def test_pipeline_fails_with_duplicate_vin(spark_session):
    """Verifies that non-unique vehicle identity keys trigger verification failure flags."""
    duplicate_data = [
        ("DUPLICATE123", "King", "Seattle", "WA", 2024, "TESLA", "BEV", 300),
        ("DUPLICATE123", "King", "Seattle", "WA", 2024, "TESLA", "BEV", 300)  # Duplicate VIN
    ]
    df = spark_session.createDataFrame(duplicate_data, schema=TEST_SCHEMA)

    check = Check(spark_session, CheckLevel.Error, "UnitTesting")
    result = (
        VerificationSuite(spark_session)
        .onData(df)
        .addCheck(check.isUnique("VIN"))
        .run()
    )

    assert result.status == "Error"
