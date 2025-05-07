from pyspark.sql.functions import col, count, date_format, date_trunc, lit
import re

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns count of new ads in each week, grouped by specified variables.

    Parameters:
    - groupByVariableList: list of grouping column names
    - Textkernel_df: input Spark DataFrame
    - date_from: string, starting date in 'YYYY-MM-DD'
    - date_to: string, ending date in 'YYYY-MM-DD'

    Returns:
    - Spark DataFrame with weekly new ads grouped by specified variables
    """
    # Validate dates
    if not re.match(r"\d{4}-\d{2}-\d{2}", date_from) or not re.match(r"\d{4}-\d{2}-\d{2}", date_to):
        raise ValueError("Date format must be YYYY-MM-DD")

    # Filter date range
    Textkernel_df = Textkernel_df.filter((col("date") >= lit(date_from)) & (col("date") <= lit(date_to)))

    # Add a week label using ISO week logic compatible with Spark 3
    Textkernel_df = Textkernel_df.withColumn("week_label", date_format(date_trunc("week", col("date")), "yyyy-ww"))

    # Create ordered list of weeks
    Dates = (
        Textkernel_df
        .select("week_label")
        .dropDuplicates()
        .sort("week_label")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Group and pivot the data
    if isinstance(groupByVariableList, list):
        Textkernel_df = (
            Textkernel_df
            .groupBy(groupByVariableList + ["week_label"])
            .agg(count("job_id").alias("count"))
            .groupBy(groupByVariableList)
            .pivot("week_label", Dates)
            .sum("count")
        )

    # Ensure final DataFrame has ordered columns
    Textkernel_df = Textkernel_df.select(groupByVariableList + Dates)

    return Textkernel_df
