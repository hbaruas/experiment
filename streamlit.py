import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Define file paths
file_path_ana_input = "D:/ERIC/Opticord_ouput/procSU_Unbalinputs_check Comparison_03.12.23_vs_04.12.23.xlsx"
file_path_ana_input_previous_current = "D:/ERIC/Opticord_ouput/Previous_Current/procSU_Unbalinputs_check Comparison_04.12.23_vs_05.12.23.xlsx"
high_level_checks_ana23_config_path = "D:/ERIC/Config_data/High_level_checks_ana23_config.csv"
low_level_checks_ana23_config_path = "D:/ERIC/Config_data/Low_level_checks_ANA23_config.csv"
high_level_checks_pre_day_config_path = "D:/ERIC/Config_data/High_level_checks_Pre_day_config.csv"
low_level_checks_pre_day_config_path = "D:/ERIC/Config_data/Low_level_checks_Prev_day_config.csv"

# Helper function to create 'full_name' column
def create_full_name_column(df):
    df['full_name'] = df['Sector'].astype(str) + df['Transaction'].astype(str) + df['Industry'].astype(str) + df['Product'].astype(str)
    cols = ['full_name'] + [col for col in df.columns if col != 'full_name']
    return df[cols]

# Helper function to calculate additional columns
def calculate_additional_columns(df, filter_years, largest_years, diff_years):
    df.columns = df.columns.str.strip()
    filter_columns = [col for col in df.columns if col.isdigit() and int(col) in filter_years]
    largest_columns = [col for col in df.columns if col.isdigit() and int(col) in largest_years]
    diff_columns = [col for col in df.columns if col.isdigit() and int(col) in diff_years]

    df['filter'] = df[filter_columns].apply(lambda x: (x != 0).sum(), axis=1)
    df['largest'] = df[largest_columns].apply(lambda x: pd.to_numeric(x, errors='coerce').abs().max(), axis=1)
    df['Diff'] = df[diff_columns].apply(lambda x: (x != 0).sum(), axis=1)
    df['largest_diff'] = df[diff_columns].apply(lambda x: pd.to_numeric(x, errors='coerce').abs().max(), axis=1)
    return df

# Load datasets
st.header("Data Loading")
with st.spinner("Loading data..."):
    Input_Curr_minus_ANA23 = pd.read_excel(file_path_ana_input, sheet_name='Difference (A)')
    Input_Curr_minus_ANA23 = create_full_name_column(Input_Curr_minus_ANA23)
    Input_Curr_minus_ANA23['filter'] = Input_Curr_minus_ANA23.iloc[:, -3:-1].apply(lambda x: (x != 0).sum(), axis=1)

    High_level_checks_ana23_config = pd.read_csv(high_level_checks_ana23_config_path, header=None, names=['full_name'])
    High_level_checks_ana23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(High_level_checks_ana23_config['full_name'])].drop(columns=['filter'])

    Low_level_checks_ana23_config = pd.read_csv(low_level_checks_ana23_config_path, header=None, names=['full_name'])
    Low_level_checks_ANA23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(Low_level_checks_ana23_config['full_name'])].drop(columns=['filter'])

    Input_Curr_minus_previous = pd.read_excel(file_path_ana_input_previous_current, sheet_name='Difference (A)')
    Input_Curr_minus_previous = create_full_name_column(Input_Curr_minus_previous)
    Input_Curr_minus_previous['filter_1'] = Input_Curr_minus_previous.iloc[:, -3:].apply(lambda x: (x != 0).sum(), axis=1)

    ANA23 = pd.read_excel(file_path_ana_input, sheet_name='Pre-Change (A)')
    ANA23 = create_full_name_column(ANA23)

    Previous = pd.read_excel(file_path_ana_input_previous_current, sheet_name='Pre-Change (A)')
    Previous = create_full_name_column(Previous)

    Current = pd.read_excel(file_path_ana_input_previous_current, sheet_name='Post-Change (A)')
    Current = create_full_name_column(Current)

    st.success("Data loaded successfully!")

# Define year ranges for calculations
filter_years = list(range(1997, 2022))
largest_years = list(range(1997, 2022))
diff_years = [2020, 2021, 2022]

# Perform calculations for additional columns
High_level_checks_ana23 = calculate_additional_columns(High_level_checks_ana23, filter_years, largest_years, diff_years)
Low_level_checks_ANA23 = calculate_additional_columns(Low_level_checks_ANA23, filter_years, largest_years, diff_years)

# Display datasets
st.header("Datasets")
st.subheader("High Level Checks - ANA23")
st.dataframe(High_level_checks_ana23)

st.subheader("Low Level Checks - ANA23")
st.dataframe(Low_level_checks_ANA23)

# Interactive filtering and charting
st.header("Interactive Charts")

sector = st.selectbox("Select Sector", ANA23['Sector'].unique())
industry = st.selectbox("Select Industry", ANA23['Industry'].unique())
product = st.selectbox("Select Product", ANA23['Product'].unique())
transaction = st.selectbox("Select Transaction", ANA23['Transaction'].unique())

filtered_ANA23 = ANA23[(ANA23['Sector'] == sector) &
                       (ANA23['Industry'] == industry) &
                       (ANA23['Product'] == product) &
                       (ANA23['Transaction'] == transaction)]

filtered_Current = Current[(Current['Sector'] == sector) &
                           (Current['Industry'] == industry) &
                           (Current['Product'] == product) &
                           (Current['Transaction'] == transaction)]

filtered_Previous = Previous[(Previous['Sector'] == sector) &
                             (Previous['Industry'] == industry) &
                             (Previous['Product'] == product) &
                             (Previous['Transaction'] == transaction)]

# Determine available years
year_columns = [str(year) for year in range(1997, 2025)]
available_years = sorted(set(year_columns) & set(filtered_ANA23.columns) & set(filtered_Current.columns) & set(filtered_Previous.columns))

# Growth rate calculations
ana23_values = filtered_ANA23[available_years].iloc[0]
current_values = filtered_Current[available_years].iloc[0]
previous_values = filtered_Previous[available_years].iloc[0]

ana23_growth = [0] + [((ana23_values[i] - ana23_values[i - 1]) / ana23_values[i - 1] * 100 if ana23_values[i - 1] != 0 else 0) for i in range(1, len(available_years))]
current_growth = [0] + [((current_values[i] - current_values[i - 1]) / current_values[i - 1] * 100 if current_values[i - 1] != 0 else 0) for i in range(1, len(available_years))]
previous_growth = [0] + [((previous_values[i] - previous_values[i - 1]) / previous_values[i - 1] * 100 if previous_values[i - 1] != 0 else 0) for i in range(1, len(available_years))]

# Line chart
line_chart = go.Figure()
line_chart.add_trace(go.Scatter(x=available_years, y=ana23_values, mode='lines+markers', name='ANA23'))
line_chart.add_trace(go.Scatter(x=available_years, y=current_values, mode='lines+markers', name='Current'))
line_chart.add_trace(go.Scatter(x=available_years, y=previous_values, mode='lines+markers', name='Previous'))

st.plotly_chart(line_chart)

# Bar chart for growth rates
bar_chart = go.Figure()
bar_chart.add_trace(go.Bar(x=available_years, y=ana23_growth, name='ANA23 Growth'))
bar_chart.add_trace(go.Bar(x=available_years, y=current_growth, name='Current Growth'))
bar_chart.add_trace(go.Bar(x=available_years, y=previous_growth, name='Previous Growth'))

st.plotly_chart(bar_chart)
