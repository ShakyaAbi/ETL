def load_to_postgres(df, table_name: str):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/spotifydb") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "yourpassword") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
