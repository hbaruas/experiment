import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Set base directory for relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define relative paths for directories
ANA_DIR = os.path.join(BASE_DIR, "ANA")
PREVIOUS_CURRENT_DIR = os.path.join(BASE_DIR, "Previous_Current")
CONFIG_DIR = os.path.join(BASE_DIR, "Config_data")

# Config file paths
HIGH_LEVEL_ANA23_CONFIG_PATH = os.path.join(CONFIG_DIR, "High_level_checks_ana23_config.csv")
LOW_LEVEL_ANA23_CONFIG_PATH = os.path.join(CONFIG_DIR, "Low_level_checks_ANA23_config.csv")
HIGH_LEVEL_PRE_DAY_CONFIG_PATH = os.path.join(CONFIG_DIR, "High_level_checks_Pre_day_config.csv")
LOW_LEVEL_PRE_DAY_CONFIG_PATH = os.path.join(CONFIG_DIR, "Low_level_checks_Prev_day_config.csv")

# Helper function to ensure only one file exists in a directory
def get_single_file_from_directory(directory):
    files = [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
    if len(files) != 1:
        st.error(f"Error: Expected exactly one file in the directory '{directory}', but found {len(files)}.")
        st.stop()
    return os.path.join(directory, files[0])

# Get the single file from ANA and Previous_Current directories
ANA_PATH = get_single_file_from_directory(ANA_DIR)
PREVIOUS_CURRENT_PATH = get_single_file_from_directory(PREVIOUS_CURRENT_DIR)

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
    # Input Curr Minus ANA23
    Input_Curr_minus_ANA23 = pd.read_excel(ANA_PATH, sheet_name='Difference (A)')
    Input_Curr_minus_ANA23 = create_full_name_column(Input_Curr_minus_ANA23)
    Input_Curr_minus_ANA23['filter'] = Input_Curr_minus_ANA23.iloc[:, -3:-1].apply(lambda x: (x != 0).sum(), axis=1)

    # High and Low Level Checks for ANA23
    High_level_checks_ana23_config = pd.read_csv(HIGH_LEVEL_ANA23_CONFIG_PATH, header=None, names=['full_name'])
    High_level_checks_ana23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(High_level_checks_ana23_config['full_name'])].drop(columns=['filter'])

    Low_level_checks_ana23_config = pd.read_csv(LOW_LEVEL_ANA23_CONFIG_PATH, header=None, names=['full_name'])
    Low_level_checks_ANA23 = Input_Curr_minus_ANA23[Input_Curr_minus_ANA23['full_name'].isin(Low_level_checks_ana23_config['full_name'])].drop(columns=['filter'])

    # Input Curr Minus Previous
    Input_Curr_minus_previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name='Difference (A)')
    Input_Curr_minus_previous = create_full_name_column(Input_Curr_minus_previous)
    Input_Curr_minus_previous['filter_1'] = Input_Curr_minus_previous.iloc[:, -3:].apply(lambda x: (x != 0).sum(), axis=1)

    # High and Low Level Checks for Previous
    High_level_checks_Pre_day_config = pd.read_csv(HIGH_LEVEL_PRE_DAY_CONFIG_PATH, header=None, names=['full_name'])
    High_level_checks_Pre_day = Input_Curr_minus_previous[Input_Curr_minus_previous['full_name'].isin(High_level_checks_Pre_day_config['full_name'])].drop(columns=['filter_1'])

    Low_level_checks_Pre_day_config = pd.read_csv(LOW_LEVEL_PRE_DAY_CONFIG_PATH, header=None, names=['full_name'])
    low_level_checks_Pre_day = Input_Curr_minus_previous[Input_Curr_minus_previous['full_name'].isin(Low_level_checks_Pre_day_config['full_name'])].drop(columns=['filter_1'])

    # ANA23, Previous, and Current datasets
    ANA23 = pd.read_excel(ANA_PATH, sheet_name='Pre-Change (A)')
    ANA23 = create_full_name_column(ANA23)

    Previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name='Pre-Change (A)')
    Previous = create_full_name_column(Previous)

    Current = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name='Post-Change (A)')
    Current = create_full_name_column(Current)

    st.success("Data loaded successfully!")

# Define year ranges for calculations
filter_years = list(range(1997, 2022))
largest_years = list(range(1997, 2022))
diff_years = [2020, 2021, 2022]

# Perform calculations for additional columns
High_level_checks_ana23 = calculate_additional_columns(High_level_checks_ana23, filter_years, largest_years, diff_years)
Low_level_checks_ANA23 = calculate_additional_columns(Low_level_checks_ANA23, filter_years, largest_years, diff_years)
High_level_checks_Pre_day = calculate_additional_columns(High_level_checks_Pre_day, filter_years, largest_years, diff_years)
low_level_checks_Pre_day = calculate_additional_columns(low_level_checks_Pre_day, filter_years, largest_years, diff_years)

# Display datasets
st.header("Datasets")

st.subheader("Input Curr Minus ANA23")
st.dataframe(Input_Curr_minus_ANA23)

st.subheader("High Level Checks - ANA23")
st.dataframe(High_level_checks_ana23)

st.subheader("Low Level Checks - ANA23")
st.dataframe(Low_level_checks_ANA23)

st.subheader("Input Curr Minus Previous")
st.dataframe(Input_Curr_minus_previous)

st.subheader("High Level Checks - Previous")
st.dataframe(High_level_checks_Pre_day)

st.subheader("Low Level Checks - Previous")
st.dataframe(low_level_checks_Pre_day)


# Load files
ANA_PATH = get_single_file_from_directory(ANA_DIR)
PREVIOUS_CURRENT_PATH = get_single_file_from_directory(PREVIOUS_CURRENT_DIR)

# Cache datasets for faster filtering
@st.cache_data
def load_datasets():
    ANA23 = pd.read_excel(ANA_PATH, sheet_name="Pre-Change (A)")
    Previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Pre-Change (A)")
    Current = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Post-Change (A)")

    # Create 'Series' column
    for df in [ANA23, Previous, Current]:
        df["Series"] = df["Sector"].astype(str) + df["Transaction"].astype(str) + df["Industry"].astype(str) + df["Product"].astype(str)
        cols = ["Series"] + [col for col in df.columns if col != "Series"]
        df = df[cols]

    return ANA23, Previous, Current

# Load data
ANA23, Previous, Current = load_datasets()

# Strip whitespace and ensure columns are strings
ANA23.columns = ANA23.columns.astype(str).str.strip()
Previous.columns = Previous.columns.astype(str).str.strip()
Current.columns = Current.columns.astype(str).str.strip()

# Define year columns
year_columns = [str(year) for year in range(1997, 2025)]

# User inputs for filtering
st.sidebar.header("Filter Options")
sector = st.sidebar.selectbox("Select Sector", ANA23["Sector"].unique())
industry = st.sidebar.selectbox("Select Industry", ANA23["Industry"].unique())
product = st.sidebar.selectbox("Select Product", ANA23["Product"].unique())
transaction = st.sidebar.selectbox("Select Transaction", ANA23["Transaction"].unique())

# Filter datasets based on user selection
@st.cache_data
def filter_datasets(sector, industry, product, transaction):
    filtered_ANA23 = ANA23[
        (ANA23["Sector"] == sector)
        & (ANA23["Industry"] == industry)
        & (ANA23["Product"] == product)
        & (ANA23["Transaction"] == transaction)
    ]
    filtered_Current = Current[
        (Current["Sector"] == sector)
        & (Current["Industry"] == industry)
        & (Current["Product"] == product)
        & (Current["Transaction"] == transaction)
    ]
    filtered_Previous = Previous[
        (Previous["Sector"] == sector)
        & (Previous["Industry"] == industry)
        & (Previous["Product"] == product)
        & (Previous["Transaction"] == transaction)
    ]
    return filtered_ANA23, filtered_Current, filtered_Previous

filtered_ANA23, filtered_Current, filtered_Previous = filter_datasets(sector, industry, product, transaction)

# Check if data is available for the selected combination
if filtered_ANA23.empty or filtered_Current.empty or filtered_Previous.empty:
    st.warning("No data available for the selected combination.")
    st.stop()

# Determine available years
available_year_columns_ana23 = [year for year in year_columns if year in filtered_ANA23.columns]
available_year_columns_current = [year for year in year_columns if year in filtered_Current.columns]
available_year_columns_previous = [year for year in year_columns if year in filtered_Previous.columns]

common_year_columns = sorted(
    set(available_year_columns_ana23)
    .union(available_year_columns_current)
    .union(available_year_columns_previous)
)


# Ensure numeric data in the filtered DataFrames
ana23_values = pd.to_numeric(filtered_ANA23[common_year_columns].iloc[0], errors="coerce").fillna(0)
current_values = pd.to_numeric(filtered_Current[common_year_columns].iloc[0], errors="coerce").fillna(0)
previous_values = pd.to_numeric(filtered_Previous[common_year_columns].iloc[0], errors="coerce").fillna(0)

# Calculate growth rates
def calculate_growth_rates(values):
    # Ensure values are numeric
    values = pd.to_numeric(values, errors="coerce").fillna(0)
    return [0] + [
        (((values[i] - values[i - 1]) / values[i - 1]) * 100 if values[i - 1] != 0 else 0)
        for i in range(1, len(values))
    ]

ana23_growth_rates = calculate_growth_rates(ana23_values)
current_growth_rates = calculate_growth_rates(current_values)
previous_growth_rates = calculate_growth_rates(previous_values)

# Display selected datasets
st.subheader("Filtered Datasets")
st.write("**Filtered ANA23:**")
st.dataframe(filtered_ANA23)
st.write("**Filtered Current:**")
st.dataframe(filtered_Current)
st.write("**Filtered Previous:**")
st.dataframe(filtered_Previous)

# Line Chart
st.subheader("Line Chart: Yearly Comparison")
line_chart = go.Figure()
line_chart.add_trace(go.Scatter(x=common_year_columns, y=ana23_values, mode="lines+markers", name="ANA23"))
line_chart.add_trace(go.Scatter(x=common_year_columns, y=current_values, mode="lines+markers", name="Current"))
line_chart.add_trace(go.Scatter(x=common_year_columns, y=previous_values, mode="lines+markers", name="Previous"))
line_chart.update_layout(title="Yearly Comparison for Levels", xaxis_title="Year", yaxis_title="Values")
st.plotly_chart(line_chart)

# Bar Chart for Growth Rates
st.subheader("Bar Chart: Yearly Growth Rates")
bar_chart = go.Figure()
bar_chart.add_trace(
    go.Bar(x=common_year_columns, y=ana23_growth_rates, name="ANA23 Growth Rate", text=ana23_growth_rates, textposition="outside")
)
bar_chart.add_trace(
    go.Bar(x=common_year_columns, y=current_growth_rates, name="Current Growth Rate", text=current_growth_rates, textposition="outside")
)
bar_chart.add_trace(
    go.Bar(x=common_year_columns, y=previous_growth_rates, name="Previous Growth Rate", text=previous_growth_rates, textposition="outside")
)
bar_chart.update_layout(title="Yearly Growth Rate Comparison", xaxis_title="Year", yaxis_title="Growth Rate (%)", barmode="group")
st.plotly_chart(bar_chart)

