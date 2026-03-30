def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
        .option("dbtable", "weather_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
query = result.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()