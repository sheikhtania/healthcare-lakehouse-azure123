silver_df = spark.table("main.default.silver_patient_clean")

display(silver_df)

from pyspark.sql.functions import avg, count

gold_df = (
    silver_df.groupBy("department")
    .agg(
        avg("wait_minutes").alias("avg_wait_minutes"),
        count("patient_id").alias("total_patients")
    )
)

from pyspark.sql.functions import date_format

gold_daily = (
    silver_df
    .withColumn("date", date_format("admission_time", "yyyy-MM-dd"))
    .groupBy("date", "department")
    .agg(
        avg("wait_minutes").alias("avg_wait_minutes"),
        count("*").alias("daily_patients")
    )
)

gold_df.write.format("delta").mode("overwrite").saveAsTable("main.default.gold_patient_metrics")

gold_daily.write.format("delta").mode("overwrite").saveAsTable("main.default.gold_daily_metrics")

spark.sql("SELECT * FROM main.default.gold_patient_metrics").show()

gold_df.orderBy("avg_wait_minutes", ascending=False).show()