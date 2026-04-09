bronze_df = spark.table("main.default.bronze_patient_raw")

display(bronze_df)

from pyspark.sql.functions import col, to_timestamp, unix_timestamp

silver_df = (
    bronze_df
    .withColumn("admission_time", to_timestamp("admission_time"))
    .withColumn("discharge_time", to_timestamp("discharge_time"))
    .withColumn(
        "wait_minutes",
        (unix_timestamp("discharge_time") - unix_timestamp("admission_time")) / 60
    )
    .dropna(subset=["patient_id", "admission_time", "discharge_time"])
)

# Remove negative wait times (bad data)
silver_df = silver_df.filter(col("wait_minutes") >= 0)

# Remove duplicates
silver_df = silver_df.dropDuplicates()

silver_df.write.format("delta").mode("overwrite").saveAsTable("main.default.silver_patient_clean")

spark.sql("SELECT * FROM main.default.silver_patient_clean").show()

silver_df.printSchema()