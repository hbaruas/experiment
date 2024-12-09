# Strip whitespace and ensure columns are strings
ANA23.columns = ANA23.columns.astype(str).str.strip()
Current.columns = Current.columns.astype(str).str.strip()

# Define year columns
year_columns = [str(year) for year in range(1997, 2023)]

# Dropdowns to select filter options
Sector_dropdown = widgets.Dropdown(options=ANA23['Sector'].unique(), description='Sector')
Industry_dropdown = widgets.Dropdown(options=ANA23['Industry'].unique(), description='Industry')
product_dropdown = widgets.Dropdown(options=ANA23['Product'].unique(), description='Product')
Transaction_dropdown = widgets.Dropdown(options=ANA23['Transaction'].unique(), description='Transaction')

def plot_filtered_data(sector, industry, product, transaction):
    # Filter data based on selection
    filtered_ANA23 = ANA23[(ANA23['Sector'] == sector) &
                           (ANA23['Industry'] == industry) &
                           (ANA23['Product'] == product) &
                           (ANA23['Transaction'] == transaction)]

    filtered_current = Current[(Current['Sector'] == sector) &
                               (Current['Industry'] == industry) &
                               (Current['Product'] == product) &
                               (Current['Transaction'] == transaction)]

    if filtered_ANA23.empty or filtered_current.empty:
        print("No data available for the selected combination")
        return

    # Use only available year columns in filtered data
    available_year_columns_ana23 = [year for year in year_columns if year in filtered_ANA23.columns]
    available_year_columns_current = [year for year in year_columns if year in filtered_current.columns]

    # Create temporary DataFrames for plotting
    temp_ANA23 = filtered_ANA23[available_year_columns_ana23].apply(pd.to_numeric, errors='coerce').fillna(0)
    temp_current = filtered_current[available_year_columns_current].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Access row values safely for plotting
    ana23_values = temp_ANA23.iloc[0]
    current_values = temp_current.iloc[0]

    # Create line chart
    fig = go.Figure()

    # Add line for ANA23
    fig.add_trace(go.Scatter(x=available_year_columns_ana23, y=ana23_values,
                             mode='lines+markers', name='ANA23'))

    # Add line for Current
    fig.add_trace(go.Scatter(x=available_year_columns_current, y=current_values,
                             mode='lines+markers', name='Current'))

    # Update the layout of the chart
    fig.update_layout(title='Yearly Comparison for Levels',
                      xaxis_title='Year',
                      yaxis_title='Values',
                      xaxis_tickangle=-45)

    # Display the chart
    fig.show()

    # Display original filtered data (with null and '.' intact)
    with pd.option_context('display.max_columns', None):
        print("Filtered ANA23 DataFrame (Original):")
        display(filtered_ANA23)

        print("Filtered Current DataFrame (Original):")
        display(filtered_current)

# Link widgets with the function
ui = widgets.HBox([Industry_dropdown, product_dropdown, Transaction_dropdown, Sector_dropdown])
out = widgets.interactive_output(plot_filtered_data, {
    'sector': Sector_dropdown,
    'industry': Industry_dropdown,
    'product': product_dropdown,
    'transaction': Transaction_dropdown
})

# Display the widgets and the output
display(ui, out)
