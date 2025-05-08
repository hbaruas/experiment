from pyspark.sql.functions import col, to_date, date_trunc, expr

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    # Validate date format
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_from) or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_to):
        raise ValueError("Dates must be in YYYY-MM-DD format")

    # Filter data within range
    Textkernel_df = Textkernel_df.filter(col("date").between(date_from, date_to))

    # Add ISO-compliant year-week and week start
    Textkernel_df = Textkernel_df \
        .withColumn("year_week_label", expr("date_format(date, 'YYYY-ww')")) \
        .withColumn("week_start_date", date_trunc("week", col("date")))

    # Create ordered list of unique weekly labels
    Dates = Textkernel_df.select("year_week_label", "week_start_date") \
        .dropDuplicates() \
        .sort("week_start_date") \
        .select("year_week_label") \
        .rdd.flatMap(lambda x: x).collect()

    # Pivot table
    if isinstance(groupByVariableList, list):
        Textkernel_df = Textkernel_df.groupBy(groupByVariableList) \
            .pivot("year_week_label", Dates) \
            .agg(expr("count(job_id)")) \
            .orderBy(groupByVariableList)
    else:
        print("groupByVariableList must be a list.")
        return

    # Reapply ordered columns
    Textkernel_df = Textkernel_df.toDF(*groupByVariableList, *Dates)

    return Textkernel_df
