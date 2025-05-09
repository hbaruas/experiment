from pyspark.sql.functions import col, weekofyear, year, lpad, concat_ws, date_trunc, count, lit
from collections import OrderedDict
import re

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns weekly count of new ads within date range, grouped by specified variables.

    Parameters:
    - groupByVariableList: list of columns to group by
    - Textkernel_df: Spark DataFrame with at least 'date' and 'job_id'
    - date_from: "YYYY-MM-DD" string
    - date_to: "YYYY-MM-DD" string

    Returns:
    - Pivoted Spark DataFrame with weekly new ad counts
    """

    # 1. Validate input date format
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_from) or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_to):
        raise ValueError("Dates must be in YYYY-MM-DD format")

    # 2. Filter only rows within date range
    Textkernel_df = Textkernel_df.filter(
        (col("date") >= lit(date_from)) & (col("date") <= lit(date_to))
    )

    # 3. Generate ISO-compatible week label and week start date
    Textkernel_df = Textkernel_df \
        .withColumn("iso_week", lpad(weekofyear(col("date")).cast("string"), 2, "0")) \
        .withColumn("iso_year", year(col("date")).cast("string")) \
        .withColumn("year_week_label", concat_ws("-", col("iso_year"), col("iso_week"))) \
        .withColumn("week_start_date", date_trunc("week", col("date")))

    # 4. Get list of unique week labels, ordered
    Dates = (
        Textkernel_df
        .select("year_week_label", "week_start_date")
        .dropDuplicates()
        .sort("week_start_date")
        .select("year_week_label")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # 5. Deduplicate week labels in case of data issues
    Dates = list(OrderedDict.fromkeys(Dates))  # preserves order, removes duplicates

    # 6. Pivot table with ad counts
    if isinstance(groupByVariableList, list):
        Textkernel_df = (
            Textkernel_df
            .groupBy(groupByVariableList + ["year_week_label"])
            .agg(count("job_id").alias("count"))
            .groupBy(groupByVariableList)
            .pivot("year_week_label", Dates)
            .sum("count")
        )
    else:
        raise ValueError("groupByVariableList must be a list")

    # 7. Final column ordering
    Textkernel_df = Textkernel_df.select(groupByVariableList + Dates)

    return Textkernel_df
