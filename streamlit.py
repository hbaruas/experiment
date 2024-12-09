import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Define the file paths
file_path_ana_input = "D:/ERIC/Opticord_ouput/procSU_Unbalinputs_check Comparison_03.12.23_vs_04.12.23.xlsx"
file_path_ana_input_previous_current = "D:/ERIC/Opticord_ouput/Previous_Current/procSU_Unbalinputs_check Comparison_04.12.23_vs_05.12.23.xlsx"

# Read the data
Input_Curr_minus_ANA23 = pd.read_excel(file_path_ana_input, sheet_name='Difference (A)')
High_level_checks_ana23_config_path = "D:/ERIC/Config_data/High_level_checks_ana23_config.csv"
Low_level_checks_ANA23_config_path = "D:/ERIC/Config_data/Low_level_checks_ANA23_config.csv"

# Define helper function for filtering data
def create_full_name_column(df):
    df['full_name'] = df['Sector'].astype(str) + df['Transaction'].astype(str) + df['Industry'].astype(str) + df['Product'].astype(str)
    cols = ['full_name'] + [col for col in df.columns if col != 'full_name']
    return df[cols]

# Add `full_name` and calculate the `filter` column for Input_Curr_minus_ANA23
Input_Curr_minus_ANA23 = create_full_name_column(Input_Curr_minus_ANA23)
Input_Curr_minus_ANA23['filter'] = Input_Curr_minus_ANA23.iloc[:, -3:-1].apply(lambda x: (x != 0).sum(), axis=1)

# Filter High Level Checks - ANA23
High_level_checks_ana23_config = pd.read_csv(High_level_checks_ana23_config_path, header=None, names=['full_name'])
High_level_checks_ana23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(High_level_checks_ana23_config['full_name'])]
High_level_checks_ana23 = High_level_checks_ana23.drop(columns=['filter'])

# Filter Low Level Checks - ANA23
Low_level_checks_ANA23_config = pd.read_csv(Low_level_checks_ANA23_config_path, header=None, names=['full_name'])
Low_level_checks_ANA23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(Low_level_checks_ANA23_config['full_name'])]
Low_level_checks_ANA23 = Low_level_checks_ANA23.drop(columns=['filter'])

# Read Previous and Current data
Previous = pd.read_excel(file_path_ana_input_previous_current, sheet_name='Pre-Change (A)')
Current = pd.read_excel(file_path_ana_input_previous_current, sheet_name='Post-Change (A)')
Previous = create_full_name_column(Previous)
Current = create_full_name_column(Current)

# Streamlit UI
st.title("Data Analysis Application")

# Display data sections
st.header("Input Curr Minus ANA23")
st.dataframe(Input_Curr_minus_ANA23)

st.header("High Level Checks - ANA23")
st.dataframe(High_level_checks_ana23)

st.header("Low Level Checks - ANA23")
st.dataframe(Low_level_checks_ANA23)

st.header("Previous Dataset")
st.dataframe(Previous)

st.header("Current Dataset")
st.dataframe(Current)

# Interactive Chart Section
st.header("Yearly Growth Rate and Line Charts")

# Dropdowns for filtering
sectors = st.selectbox("Select Sector", options=ANA23['Sector'].unique())
industries = st.selectbox("Select Industry", options=ANA23['Industry'].unique())
products = st.selectbox("Select Product", options=ANA23['Product'].unique())
transactions = st.selectbox("Select Transaction", options=ANA23['Transaction'].unique())

# Filter datasets based on dropdowns
filtered_ANA23 = ANA23[(ANA23['Sector'] == sectors) &
                       (ANA23['Industry'] == industries) &
                       (ANA23['Product'] == products) &
                       (ANA23['Transaction'] == transactions)]

filtered_Current = Current[(Current['Sector'] == sectors) &
                           (Current['Industry'] == industries) &
                           (Current['Product'] == products) &
                           (Current['Transaction'] == transactions)]

filtered_Previous = Previous[(Previous['Sector'] == sectors) &
                             (Previous['Industry'] == industries) &
                             (Previous['Product'] == products) &
                             (Previous['Transaction'] == transactions)]

# Ensure year columns exist in all datasets
year_columns = [str(year) for year in range(1997, 2025)]
available_year_columns = sorted(set(year_columns) &
                                 set(filtered_ANA23.columns) &
                                 set(filtered_Current.columns) &
                                 set(filtered_Previous.columns))

# Reindex datasets to include all year columns
filtered_ANA23 = filtered_ANA23.reindex(columns=available_year_columns, fill_value=0)
filtered_Current = filtered_Current.reindex(columns=available_year_columns, fill_value=0)
filtered_Previous = filtered_Previous.reindex(columns=available_year_columns, fill_value=0)

# Calculate growth rates
ana23_values = filtered_ANA23.iloc[0, :]
current_values = filtered_Current.iloc[0, :]
previous_values = filtered_Previous.iloc[0, :]

ana23_growth_rates = [0] + [((ana23_values[i] - ana23_values[i - 1]) / ana23_values[i - 1] * 100 if ana23_values[i - 1] != 0 else 0) for i in range(1, len(available_year_columns))]
current_growth_rates = [0] + [((current_values[i] - current_values[i - 1]) / current_values[i - 1] * 100 if current_values[i - 1] != 0 else 0) for i in range(1, len(available_year_columns))]
previous_growth_rates = [0] + [((previous_values[i] - previous_values[i - 1]) / previous_values[i - 1] * 100 if previous_values[i - 1] != 0 else 0) for i in range(1, len(available_year_columns))]

# Create Line Chart
line_fig = go.Figure()
line_fig.add_trace(go.Scatter(x=available_year_columns, y=ana23_values, mode='lines+markers', name='ANA23'))
line_fig.add_trace(go.Scatter(x=available_year_columns, y=current_values, mode='lines+markers', name='Current'))
line_fig.add_trace(go.Scatter(x=available_year_columns, y=previous_values, mode='lines+markers', name='Previous'))

line_fig.update_layout(title='Line Chart for Selected Filters',
                       xaxis_title='Year',
                       yaxis_title='Values',
                       xaxis_tickangle=-45)
st.plotly_chart(line_fig)

# Create Bar Chart for Growth Rates
bar_fig = go.Figure()
bar_fig.add_trace(go.Bar(x=available_year_columns, y=ana23_growth_rates, name='ANA23 Growth Rate'))
bar_fig.add_trace(go.Bar(x=available_year_columns, y=current_growth_rates, name='Current Growth Rate'))
bar_fig.add_trace(go.Bar(x=available_year_columns, y=previous_growth_rates, name='Previous Growth Rate'))

bar_fig.update_layout(title='Growth Rate Comparison',
                      xaxis_title='Year',
                      yaxis_title='Growth Rate (%)',
                      barmode='group')
st.plotly_chart(bar_fig)
