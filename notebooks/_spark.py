from __future__ import annotations

try:
    from pyspark.sql import SparkSession  # type: ignore
except Exception:
    SparkSession = None  # type: ignore


def get_spark():
    if SparkSession is None:
        return None
    return SparkSession.builder.getOrCreate()


spark = get_spark()
