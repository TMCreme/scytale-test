"""
The Spark transformation and result
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, max, split, from_utc_timestamp, count, coalesce, lit, lower
)
from pyspark.sql import Window


def transformation_with_spark(organization: str, output_path: str):

    # Initialize a Spark session
    spark = SparkSession.builder.appName(
        "GitHubDataTransformation"
        ).getOrCreate()
    # Read the respositories data
    repo_df = spark.read \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(f"{organization}/repositories").select(
            col("id").alias("repository_id"),
            col("full_name").alias("repository_full_name"),
            col("name").alias("repository_name"),
            col("owner.login").alias("repository_owner")
        )

    # Read the Pull requests data
    prs_df = spark.read \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(f"{organization}/prs").select(
            col("id").alias("pr_id"),
            col("state"),
            col("merged_at"),
            col("base.repo.name").alias("repository_name")
        )

    # Perform an Outer join on both dfs with repository name
    final_df = repo_df.withColumn(
        "organization_name", split(col("repository_full_name"), "/")[0]
    ).join(
        prs_df,
        on="repository_name",
        how="left_outer").drop("repository_full_name")

    # Convert the field to timestamp
    final_df = final_df.withColumn(
        "merged_at", from_utc_timestamp(col("merged_at"), "UTC"))

    # Define the window around the repository name
    window_spec = Window().partitionBy("repository_name")

    # Transform data to aggregate: num_prs, merged_prs
    df_transformed = final_df.withColumn(
        "num_prs", count(
            when(col("pr_id").isNotNull(), col("pr_id"))).over(window_spec)) \
        .withColumn(
            "num_prs_merged", when(
                col("merged_at").isNotNull(), coalesce(
                    count("merged_at").over(window_spec), lit(0)))) \
        .withColumn(
            "merged_at", coalesce(max("merged_at").over(window_spec)))

    # Get the compliant condition for when
    df_transformed = df_transformed.withColumn(
        "is_compliant", when(
            (col("num_prs") == col("num_prs_merged")) & (
                lower(col("repository_owner")).contains("scytale")), True)
    )

    # Drop unwanted columns
    dropped_columns = ["pr_id", "state", "name"]
    df_transformed = df_transformed.drop(*dropped_columns)

    # Drop duplicates or get uniques
    df_transformed = df_transformed.dropDuplicates()
    df_transformed.show()

    # Write the result to parquet
    df_transformed.write.parquet(output_path, mode="overwrite")

    # Stop the spark session
    spark.stop()
