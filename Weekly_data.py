def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns count of new ads in calendar weeks across the reporting period

    Parameters:
    groupByVariableList = a list of variables to produce counts by (and also order counts by)
    Textkernel_df = the spark textkernel dataset
    date_from = the date from which to start including data (inclusive)
    date_to = the date to which you would the data included (inclusive)

    Returns:
    Textkernel_df = a spark data frame of counts of new ads by specified variables
    """

    # Validate date format (YYYY-MM-DD assumed now for weekly granularity)
    if ((not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_from))) or
        (not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_to)))):
        raise IncorrectDateFormatException()

    # Filter by date range
    Textkernel_df = Textkernel_df.filter(F.col("date").between(date_from, date_to))

    # Create a weekly label column
    Textkernel_df = Textkernel_df.withColumn('week_label', F.date_format(F.col("date"), "yyyy-'W'ww"))
    Textkernel_df = Textkernel_df.withColumn('week_start_date', F.date_trunc("week", F.col("date")))

    # Create ordered list of weeks
    Dates = Textkernel_df \
        .select("week_label", "week_start_date") \
        .dropDuplicates() \
        .sort("week_start_date") \
        .select("week_label") \
        .rdd.flatMap(lambda x: x).collect()

    # Aggregate data
    if type(groupByVariableList) == list:
        Dates = groupByVariableList + Dates
        Textkernel_df = Textkernel_df \
            .groupBy(groupByVariableList) \
            .pivot("week_label") \
            .agg(count(F.col("job_id"))) \
            .orderBy(groupByVariableList)
    else:
        print("Variable list to group by needs to be configured in the form of a list")
        return

    # Apply ordered week columns
    Textkernel_df = Textkernel_df.toDF(*Dates)

    return Textkernel_df
