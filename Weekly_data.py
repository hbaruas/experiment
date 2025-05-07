from datetime import datetime, timedelta
from pyspark.sql import functions as F

def new_ads(groupByVariableList, Textkernel_df, date_from, date_to):
    """
    Returns count of new ads in each week between date_from and date_to.
    
    Parameters:
    groupByVariableList : list of column names to group by
    Textkernel_df : spark dataframe with job ads
    date_from : string, 'YYYY-MM-DD'
    date_to : string, 'YYYY-MM-DD'
    
    Returns:
    Spark DataFrame with weekly job counts, grouped and pivoted.
    """
    # Ensure the date range is valid
    start_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.strptime(date_to, '%Y-%m-%d')
    
    # Generate list of ISO week labels
    expected_weeks = []
    current_date = start_date
    while current_date <= end_date:
        iso_year, iso_week, _ = current_date.isocalendar()
        week_label = f"{iso_year}-{iso_week:02d}"
        if week_label not in expected_weeks:
            expected_weeks.append(week_label)
        current_date += timedelta(days=7)  # jump by week

    # Create week label column
    Textkernel_df = Textkernel_df.withColumn(
        "week_label",
        F.date_format(F.col("week_start_date"), "yyyy-ww")
    )

    # Group and pivot
    if type(groupByVariableList) == list:
        Dates = groupByVariableList + ["week_label"]
        Textkernel_df = (
            Textkernel_df
            .groupBy(groupByVariableList + ["week_label"])
            .agg(F.count("job_id").alias("count"))
            .groupBy(groupByVariableList)
            .pivot("week_label", expected_weeks)
            .agg(F.first("count"))
        )

        # Fill any missing columns with 0
        for week in expected_weeks:
            if week not in Textkernel_df.columns:
                Textkernel_df = Textkernel_df.withColumn(week, F.lit(0))

        # Reorder columns to match expected list
        final_cols = groupByVariableList + expected_weeks
        Textkernel_df = Textkernel_df.select(final_cols)

    return Textkernel_df
