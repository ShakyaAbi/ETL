import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, explode

def create_spark_session():
    """Create or get Spark session"""
    return SparkSession.builder.appName("ETL_Transform").getOrCreate()

def clean_and_save_data(spark, input_dir, output_dir):
    """Read extracted files, clean data, and save as Parquet"""
    # Define schemas
    artist_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("genres", ArrayType(StringType()), True)
    ])

    track_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("id_artists", ArrayType(StringType()), True),
        StructField("popularity", StringType(), True)
    ])

    rec_schema = StructType([
        StructField("id", StringType(), True),
        StructField("recommendations", ArrayType(StringType()), True)
    ])

    # Read files
    df_artists = spark.read.csv(os.path.join(input_dir, "artists.csv"), header=True, schema=artist_schema)
    df_tracks = spark.read.csv(os.path.join(input_dir, "tracks.csv"), header=True, schema=track_schema)
    df_recommend = spark.read.json(os.path.join(input_dir, "fixed_da.json"), schema=rec_schema)

    # Clean data
    df_artists = df_artists.dropDuplicates(["id"]).filter(col("id").isNotNull())
    df_tracks = df_tracks.dropDuplicates(["id"]).filter(col("id").isNotNull())
    df_recommend = df_recommend.dropDuplicates(["id"]).filter(col("id").isNotNull())

    # Save cleaned files
    df_artists.write.mode("overwrite").parquet(os.path.join(output_dir, "artists_cleaned"))
    df_tracks.write.mode("overwrite").parquet(os.path.join(output_dir, "tracks_cleaned"))
    df_recommend.write.mode("overwrite").parquet(os.path.join(output_dir, "recommendations_cleaned"))

    return df_artists, df_tracks, df_recommend

def create_master_table(output_dir, df_artists, df_tracks, df_recommend):
    """Create and save a joined master table"""
    df_tracks_exploded = df_tracks.withColumn("artist_id", explode(col("id_artists")))

    df_master = df_tracks_exploded \
        .join(df_artists, df_tracks_exploded.artist_id == df_artists.id, "left") \
        .join(df_recommend, df_tracks_exploded.id == df_recommend.id, "left")

    df_master.write.mode("overwrite").parquet(os.path.join(output_dir, "master_table"))

def create_analytics_tables(output_dir, df_artists, df_tracks, df_recommend):
    """Create and save useful analytical datasets"""
    # Exploded recommendation table
    df_recommend_exploded = df_recommend.withColumn("recommended_track_id", explode(col("recommendations"))) \
                                        .select("id", "recommended_track_id")
    df_recommend_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "recommendation_exploded"))

    # Track → Artist mapping
    df_track_artist = df_tracks.withColumn("artist_id", explode(col("id_artists"))) \
                               .select("id", "artist_id")
    df_track_artist.write.mode("overwrite").parquet(os.path.join(output_dir, "track_artist"))

    # Artist Metadata
    df_artists_metadata = df_artists.select("id", "name", "genres")
    df_artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "artist_metadata"))

    # Track Metadata
    df_tracks_metadata = df_tracks.select("id", "name", "popularity")
    df_tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "track_metadata"))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python transform/execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    try:
        spark = create_spark_session()

        df_artists, df_tracks, df_recommend = clean_and_save_data(spark, input_dir, output_dir)
        create_master_table(output_dir, df_artists, df_tracks, df_recommend)
        create_analytics_tables(output_dir, df_artists, df_tracks, df_recommend)

        print("✅ Transformation completed successfully.")
    except Exception as e:
        print(f"Error during transformation: {e}")
        sys.exit(1)
