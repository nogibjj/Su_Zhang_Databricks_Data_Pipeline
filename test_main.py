"""
Test goes here

"""
import os
import pytest

from mylib.calculator import (
    create_spark_session,
    extract_data,
    transform_data,
    load_data,
    query_data,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = create_spark_session("TestApp")
    yield spark_session
    spark_session.stop()


def test_extract():
    file_path = extract_data()
    assert os.path.exists(file_path) is True


def test_load_data(spark):
    df = load_data(spark)
    assert df is not None


def test_query(spark):
    df = load_data(spark)
    result = query_data(spark, df)
    assert result is not None


def test_transform(spark):
    df = load_data(spark)
    transformed_df = transform_data(df)
    assert transformed_df.count() > 0
