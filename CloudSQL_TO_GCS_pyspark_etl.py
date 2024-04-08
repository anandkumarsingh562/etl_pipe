from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName('demo_dataframe').getOrCreate()

airports_schema=StructType([StructField("airport_number",StringType(),True),
                            StructField("name",StringType(),True),
                            StructField("latitude",DoubleType(),True),
                            StructField("longitude",DoubleType(),True),
                            StructField("alt",IntegerType(),True),
                            StructField("tz",IntegerType(),True),
                            StructField("dst",StringType(),True)])


First_df=spark.read.format("csv").schema(airports_schema).load("gs://source_flights_table/airports_stage.csv")

First_df=First_df.withColumn("load_date",current_date())

First_df.coalesce(1).write.format("csv").save("gs://source_flights_table/airports_stage_modified.csv")



