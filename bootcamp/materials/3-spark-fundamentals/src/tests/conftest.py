import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
  return SparkSession.builder \
    .appName("chispa") \
    .master("local") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://localhost:8181") \
    .getOrCreate()