from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", True).csv("/Volumes/main/default/lakehouse_volume/patient_visits.csv")

display(df)

df.write.format("delta").mode("overwrite").saveAsTable("main.default.bronze_patient_raw")

spark.sql("SELECT * FROM main.default.bronze_patient_raw").show()

from pyspark.sql.types import *

schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("admission_time", StringType(), True),
    StructField("discharge_time", StringType(), True),
    StructField("department", StringType(), True),
    StructField("triage_level", StringType(), True)
])

df = spark.read.schema(schema).option("header", True).csv("/Volumes/main/default/lakehouse_volume/patient_visits.csv")