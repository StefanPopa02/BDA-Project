from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from string import ascii_uppercase as alphabet

if __name__ == '__main__':
    spark = SparkSession.builder.appName("BDA Project").getOrCreate()

    # DAILY
    schema_daily = StructType(
        fields=[
            StructField("LCLid", StringType(), True),
            StructField("day", DateType(), True),
            StructField("energy_median", DoubleType(), True),
            StructField("energy_mean", DoubleType(), True),
            StructField("energy_max", DoubleType(), True),
            StructField("energy_count", IntegerType(), True),
            StructField("energy_std", DoubleType(), True),
            StructField("energy_sum", DoubleType(), True),
            StructField("energy_min", DoubleType(), True),
        ])

    # daily_dataset_df = spark.read.csv("dataset/daily_dataset/daily_dataset/*", header=True, schema=schema_daily).persist()
    daily_dataset_df = spark.read.csv("dataset/daily_dataset/daily_dataset/block_0.csv", header=True, schema=schema_daily).persist()
    daily_dataset_df.createOrReplaceTempView("daily_dataset")
    print("Daily dataset ex:", daily_dataset_df.head())
    print("Number of rows:", daily_dataset_df.count())

    # HALF HOURLY
    schema_halfhourly = StructType(
        fields=[
            StructField("LCLid", StringType(), True),
            StructField("tstp", DateType(), True),
            StructField("energy(kWh/hh)", DoubleType(), True),
        ])
    # halfhourly_dataset_df = spark.read.csv("dataset/halfhourly_dataset/halfhourly_dataset/*", header=True, schema=schema_halfhourly).persist()
    halfhourly_dataset_df = spark.read.csv("dataset/halfhourly_dataset/halfhourly_dataset/block_0.csv", header=True, schema=schema_halfhourly).persist()
    halfhourly_dataset_df.createOrReplaceTempView("halfhourly_dataset")
    print("Halfhourly dataset ex:", halfhourly_dataset_df.head())
    print("Number of rows:", halfhourly_dataset_df.count())

    # HH
    fields = [StructField("LCLid", StringType(), True), StructField("day", DateType(), True)]
    for field in [StructField("hh_" + str(i), DoubleType(), True) for i in range(48)]:
        fields.append(field)
    schema_hh = StructType(fields=fields)
    # hh_dataset_df = spark.read.csv("dataset/hhblock_dataset/hhblock_dataset/*", header=True, schema=schema_hh).persist()
    hh_dataset_df = spark.read.csv("dataset/hhblock_dataset/hhblock_dataset/block_0.csv", header=True, schema=schema_hh).persist()
    hh_dataset_df.createOrReplaceTempView("hh_dataset")
    print("HH block dataset ex:", hh_dataset_df.head())
    print("Number of rows:", hh_dataset_df.count())

    # ACORN DETAILS
    fields = [
        StructField("MAIN CATEGORIES", StringType(), True),
        StructField("CATEGORIES", StringType(), True),
        StructField("REFERENCE", StringType(), True),
    ]
    for field in [StructField("ACORN-" + str(i), DoubleType(), True) for i in alphabet[:17]]:
        fields.append(field)
    schema_acorn = StructType(fields=fields)
    acorn_dataset_df = spark.read.csv("dataset/acorn_details.csv", header=True, schema=schema_acorn).persist()
    acorn_dataset_df.createOrReplaceTempView("acorn_details")
    print("ACORN DETAILS dataset ex:", acorn_dataset_df.head())
    print("Number of rows:", acorn_dataset_df.count())
