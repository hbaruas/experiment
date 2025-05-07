from pyspark.sql.functions import col, weekofyear, year, lpad, concat_ws, date_trunc, count

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns count of new ads in calendar weeks across the reporting period

    Parameters:
    groupByVariableList = a list of variables to produce counts by (and also order counts by)
    Textkernel_df = the Spark Textkernel dataset
    date_from = the date from which to start including data (inclusive) in 'YYYY-MM-DD' format
    date_to = the date to which the data is included (inclusive) in 'YYYY-MM-DD' format

    Returns:
    Textkernel_df = a Spark DataFrame of counts of new ads by specified variables
    """

    # Validate date formats (expects full dates now)
    if ((not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_from))) or
        (not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_to)))):
        raise IncorrectDateFormatException()

    # Filter by date range
    Textkernel_df = Textkernel_df.filter(col("date").between(date_from, date_to))

    # Add weekly label and start date for sorting
    Textkernel_df = Textkernel_df \
        .withColumn("week_number", lpad(weekofyear(col("date")).cast("string"), 2, "0")) \
        .withColumn("year_week_label", concat_ws("-", year(col("date")).cast("string"), col("week_number"))) \
        .withColumn("week_start_date", date_trunc("week", col("date")))

    # Create sorted list of weekly columns
    Dates = Textkernel_df \
        .select("year_week_label", "week_start_date") \
        .dropDuplicates() \
        .sort("week_start_date") \
        .select("year_week_label") \
        .rdd.flatMap(lambda x: x).collect()

    # Group and pivot the data
    if type(groupByVariableList) == list:
        Dates = groupByVariableList + Dates
        Textkernel_df = Textkernel_df \
            .groupBy(groupByVariableList) \
            .pivot("year_week_label") \
            .agg(count(col("job_id"))) \
            .orderBy(groupByVariableList)
    else:
        print("Variable list to group by needs to be configured in the form of a list")
        return

    # Ensure final DataFrame has ordered columns
    Textkernel_df = Textkernel_df.toDF(*Dates)

    return Textkernel_df
