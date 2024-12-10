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

# Cache datasets for faster performance
@st.cache_data
def load_datasets():
    # Load ANA23, Previous, and Current datasets
    ANA23 = pd.read_excel(ANA_PATH, sheet_name="Pre-Change (A)")
    Previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Pre-Change (A)")
    Current = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Post-Change (A)")

    # Create 'Series' column
    for df in [ANA23, Previous, Current]:
        df["Series"] = df["Sector"].astype(str) + df["Transaction"].astype(str) + df["Industry"].astype(str) + df["Product"].astype(str)
        cols = ["Series"] + [col for col in df.columns if col != "Series"]
        df = df[cols]

    return ANA23, Previous, Current

# Load datasets
ANA23, Previous, Current = load_datasets()

# Strip whitespace and ensure columns are strings
for df in [ANA23, Previous, Current]:
    df.columns = df.columns.astype(str).str.strip()

# Define year columns dynamically
year_columns = [col for col in ANA23.columns if col.isdigit()]

# Sidebar filters
st.sidebar.header("Filter Options")
sector = st.sidebar.selectbox("Select Sector", ANA23["Sector"].unique())
industry = st.sidebar.selectbox("Select Industry", ANA23["Industry"].unique())
product = st.sidebar.selectbox("Select Product", ANA23["Product"].unique())
transaction = st.sidebar.selectbox("Select Transaction", ANA23["Transaction"].unique())

# Cache filtered datasets for faster updates
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

# Filter data based on user selection
filtered_ANA23, filtered_Current, filtered_Previous = filter_datasets(sector, industry, product, transaction)

# Check if data is available for the selected combination
if filtered_ANA23.empty or filtered_Current.empty or filtered_Previous.empty:
    st.warning("No data available for the selected combination.")
    st.stop()

# Determine available years dynamically for each dataset
available_year_columns_ana23 = [col for col in year_columns if col in filtered_ANA23.columns]
available_year_columns_current = [col for col in year_columns if col in filtered_Current.columns]
available_year_columns_previous = [col for col in year_columns if col in filtered_Previous.columns]

# Combine all available years
common_year_columns = sorted(
    set(available_year_columns_ana23)
    .union(available_year_columns_current)
    .union(available_year_columns_previous)
)

# Ensure consistent numeric data
ana23_values = filtered_ANA23[common_year_columns].iloc[0].fillna(0).astype(float)
current_values = filtered_Current[common_year_columns].iloc[0].fillna(0).astype(float)
previous_values = filtered_Previous[common_year_columns].iloc[0].fillna(0).astype(float)

# Calculate growth rates
def calculate_growth_rates(values):
    return [0] + [
        (((values[i] - values[i - 1]) / values[i - 1]) * 100 if values[i - 1] != 0 else 0)
        for i in range(1, len(values))
    ]

ana23_growth_rates = calculate_growth_rates(ana23_values)
current_growth_rates = calculate_growth_rates(current_values)
previous_growth_rates = calculate_growth_rates(previous_values)

# Display filtered datasets
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




import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Define directories for relative paths
BASE_DIR = os.getcwd()
ANA_DIR = os.path.join(BASE_DIR, "ANA")
PREVIOUS_CURRENT_DIR = os.path.join(BASE_DIR, "Previous_Current")

# Helper function to ensure only one file exists in a directory
def get_single_file_from_directory(directory):
    files = [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
    if len(files) != 1:
        st.error(f"Error: Expected exactly one file in the directory '{directory}', but found {len(files)}.")
        st.stop()
    return os.path.join(directory, files[0])

# Function to load datasets
def load_datasets():
    # Get file paths
    ANA_PATH = get_single_file_from_directory(ANA_DIR)
    PREVIOUS_CURRENT_PATH = get_single_file_from_directory(PREVIOUS_CURRENT_DIR)

    # Load datasets
    ANA23 = pd.read_excel(ANA_PATH, sheet_name="Pre-Change (A)")
    Current = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Post-Change (A)")
    Previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Pre-Change (A)")

    # Create "Series" column for consistency
    for df in [ANA23, Current, Previous]:
        df["Series"] = df["Sector"].astype(str) + df["Transaction"].astype(str) + df["Industry"].astype(str) + df["Product"].astype(str)
        cols = ["Series"] + [col for col in df.columns if col != "Series"]
        df = df[cols]

    # Strip whitespace and standardize column names
    for df in [ANA23, Current, Previous]:
        df.columns = df.columns.astype(str).str.strip()

    # Define year columns
    year_columns = [col for col in ANA23.columns if col.isdigit()]

    return ANA23, Current, Previous, year_columns

# Use session state to store data
if "datasets" not in st.session_state:
    with st.spinner("Loading datasets..."):
        st.session_state.datasets = load_datasets()
        st.success("Data loaded successfully!")

# Retrieve datasets from session state
ANA23, Current, Previous, year_columns = st.session_state.datasets

# Sidebar filters
st.sidebar.header("Filter Options")
sector = st.sidebar.selectbox("Select Sector", ANA23["Sector"].unique())
industry = st.sidebar.selectbox("Select Industry", ANA23["Industry"].unique())
product = st.sidebar.selectbox("Select Product", ANA23["Product"].unique())
transaction = st.sidebar.selectbox("Select Transaction", ANA23["Transaction"].unique())

# Filter datasets dynamically
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

# Apply filtering
filtered_ANA23, filtered_Current, filtered_Previous = filter_datasets(sector, industry, product, transaction)

if filtered_ANA23.empty or filtered_Current.empty or filtered_Previous.empty:
    st.warning("No data available for the selected combination.")
    st.stop()

# Convert columns for numeric calculations while keeping original DataFrames intact
def clean_numeric_data(df, year_columns):
    numeric_df = df.copy()
    for col in year_columns:
        numeric_df[col] = pd.to_numeric(numeric_df[col], errors="coerce").fillna(0)  # Replace non-numeric with 0
    return numeric_df

numeric_ANA23 = clean_numeric_data(filtered_ANA23, year_columns)
numeric_Current = clean_numeric_data(filtered_Current, year_columns)
numeric_Previous = clean_numeric_data(filtered_Previous, year_columns)

# Calculate growth rates
def calculate_growth_rates(df, year_columns):
    rates = []
    for i in range(1, len(year_columns)):
        previous = df[year_columns[i - 1]]
        current = df[year_columns[i]]
        growth = ((current - previous) / previous) * 100 if previous != 0 else 0
        rates.append(growth)
    return [0] + rates  # Start with 0% growth for the first year

ana23_growth_rates = calculate_growth_rates(numeric_ANA23.iloc[0], year_columns)
current_growth_rates = calculate_growth_rates(numeric_Current.iloc[0], year_columns)
previous_growth_rates = calculate_growth_rates(numeric_Previous.iloc[0], year_columns)

# Display filtered datasets
st.subheader("Filtered Datasets")
st.write("**Filtered ANA23 (Original):**")
st.dataframe(filtered_ANA23)
st.write("**Filtered Current (Original):**")
st.dataframe(filtered_Current)
st.write("**Filtered Previous (Original):**")
st.dataframe(filtered_Previous)

# Line Chart
st.subheader("Line Chart: Yearly Comparison")
fig_line = go.Figure()
fig_line.add_trace(go.Scatter(x=year_columns, y=numeric_ANA23.iloc[0][year_columns], mode="lines+markers", name="ANA23"))
fig_line.add_trace(go.Scatter(x=year_columns, y=numeric_Current.iloc[0][year_columns], mode="lines+markers", name="Current"))
fig_line.add_trace(go.Scatter(x=year_columns, y=numeric_Previous.iloc[0][year_columns], mode="lines+markers", name="Previous"))
fig_line.update_layout(title="Yearly Comparison", xaxis_title="Year", yaxis_title="Values")
st.plotly_chart(fig_line)

# Bar Chart for Growth Rates
st.subheader("Bar Chart: Growth Rates")
fig_bar = go.Figure()
fig_bar.add_trace(go.Bar(x=year_columns, y=ana23_growth_rates, name="ANA23 Growth Rate"))
fig_bar.add_trace(go.Bar(x=year_columns, y=current_growth_rates, name="Current Growth Rate"))
fig_bar.add_trace(go.Bar(x=year_columns, y=previous_growth_rates, name="Previous Growth Rate"))
fig_bar.update_layout(title="Growth Rates Comparison", xaxis_title="Year", yaxis_title="Growth Rate (%)", barmode="group")
st.plotly_chart(fig_bar)
