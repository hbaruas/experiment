import re
from pyspark.sql.functions import col, weekofyear, year, lpad, concat_ws, date_trunc, count

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns count of new ads in calendar weeks across the reporting period.

    Parameters:
    - groupByVariableList: list of variables to group by
    - Textkernel_df: Spark DataFrame containing job adverts with a 'date' column
    - date_from: string (YYYY-MM-DD), start date (inclusive)
    - date_to: string (YYYY-MM-DD), end date (inclusive)

    Returns:
    - Textkernel_df: Spark DataFrame pivoted to show counts of new ads by week
    """

    # Validate date format
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_from) or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_to):
        raise ValueError("Dates must be in YYYY-MM-DD format")

    # Filter data within range
    Textkernel_df = Textkernel_df.filter(col("date").between(date_from, date_to))

    # Generate weekly label and week start date
    Textkernel_df = Textkernel_df \
        .withColumn("week_number", lpad(weekofyear(col("date")).cast("string"), 2, "0")) \
        .withColumn("year_week_label", concat_ws("-", year(col("date")), col("week_number"))) \
        .withColumn("week_start_date", date_trunc("week", col("date")))

    # Create ordered list of weekly labels
    Dates = Textkernel_df \
        .select("year_week_label", "week_start_date") \
        .dropDuplicates() \
        .sort("week_start_date") \
        .select("year_week_label") \
        .rdd.flatMap(lambda x: x).collect()

    # Aggregate by groupByVariableList and pivot by week
    if isinstance(groupByVariableList, list):
        Dates = groupByVariableList + Dates
        Textkernel_df = Textkernel_df \
            .groupBy(groupByVariableList) \
            .pivot("year_week_label") \
            .agg(count(col("job_id"))) \
            .orderBy(groupByVariableList)
    else:
        print("groupByVariableList must be a list.")
        return

    # Apply ordered columns
    Textkernel_df = Textkernel_df.toDF(*Dates)

    return Textkernel_df
