from pyspark.sql.functions import col, datediff, lit, floor, concat_ws, lpad, count
import re
from collections import OrderedDict

def new_ads_simple_weeks(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Groups ads into fixed 7-day weeks starting from date_from, and pivots the result.
    """

    # Validate input dates
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_from) or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_to):
        raise ValueError("Dates must be in YYYY-MM-DD format")

    # Filter date range
    Textkernel_df = Textkernel_df.filter(
        (col("date") >= lit(date_from)) & (col("date") <= lit(date_to))
    )

    # Assign fixed 7-day rolling week IDs starting from date_from
    Textkernel_df = Textkernel_df.withColumn(
        "week_number",
        floor(datediff(col("date"), lit(date_from)) / 7)
    ).withColumn(
        "week_id",
        concat_ws("_", lit("week"), lpad(col("week_number").cast("string"), 3, "0"))
    )

    # Get sorted week labels
    week_labels = (
        Textkernel_df.select("week_id")
        .dropDuplicates()
        .sort("week_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    week_labels = list(OrderedDict.fromkeys(week_labels))

    # Group and pivot
    if isinstance(groupByVariableList, list):
        Textkernel_df = (
            Textkernel_df
            .groupBy(groupByVariableList + ["week_id"])
            .agg(count("job_id").alias("count"))
            .groupBy(groupByVariableList)
            .pivot("week_id", week_labels)
            .sum("count")
        )
    else:
        raise ValueError("groupByVariableList must be a list")

    # Final ordering
    Textkernel_df = Textkernel_df.select(groupByVariableList + week_labels)

    return Textkernel_df
