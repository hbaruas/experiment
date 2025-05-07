import re
from pyspark.sql.functions import col, count, date_format, date_trunc, lit

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
    # Validate date format
    if not re.match(r"\d{4}-\d{2}-\d{2}", date_from) or not re.match(r"\d{4}-\d{2}-\d{2}", date_to):
        raise ValueError("Date format must be YYYY-MM-DD")

    # Filter for the given date range
    Textkernel_df = Textkernel_df.filter((col("date") >= lit(date_from)) & (col("date") <= lit(date_to)))

    # Create a label column for each week using ISO-compatible Spark 3 syntax
    Textkernel_df = Textkernel_df.withColumn("week_label", date_format(date_trunc("week", col("date")), "yyyy-ww"))

    # Generate a sorted list of week_label values for correct pivoting
    Dates = (
        Textkernel_df
        .select("week_label")
        .dropDuplicates()
        .sort("week_label")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Group by user-defined variables and week_label, then pivot and aggregate
    if isinstance(groupByVariableList, list):
        Textkernel_df = (
            Textkernel_df
            .groupBy(groupByVariableList + ["week_label"])
            .agg(count("job_id").alias("count"))
            .groupBy(groupByVariableList)
            .pivot("week_label", Dates)
            .sum("count")
        )

    # Apply ordered column layout to avoid mismatch during downstream joins
    Textkernel_df = Textkernel_df.select(groupByVariableList + Dates)

    return Textkernel_df
