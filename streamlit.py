import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Set base directory for relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define relative paths for directories
ANA_DIR = os.path.join(BASE_DIR, "Opticord_output/ANA")
PREVIOUS_CURRENT_DIR = os.path.join(BASE_DIR, "Opticord_output/Previous_Current")
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

@st.cache_data(show_spinner=False)
def load_all_data():
    # Get file paths
    ANA_PATH = get_single_file_from_directory(ANA_DIR)
    PREVIOUS_CURRENT_PATH = get_single_file_from_directory(PREVIOUS_CURRENT_DIR)

    # Load Input Curr Minus ANA23
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

    return (Input_Curr_minus_ANA23, High_level_checks_ana23, Low_level_checks_ANA23,
            Input_Curr_minus_previous, High_level_checks_Pre_day, low_level_checks_Pre_day,
            ANA23, Previous, Current)

@st.cache_data(show_spinner=False)
def preprocess_loaded_data(Input_Curr_minus_ANA23, High_level_checks_ana23, Low_level_checks_ANA23,
                           Input_Curr_minus_previous, High_level_checks_Pre_day, low_level_checks_Pre_day,
                           ANA23, Previous, Current):

    filter_years = list(range(1997, 2022))
    largest_years = list(range(1997, 2022))
    diff_years = [2020, 2021, 2022]

    # Perform calculations for additional columns
    High_level_checks_ana23 = calculate_additional_columns(High_level_checks_ana23, filter_years, largest_years, diff_years)
    Low_level_checks_ANA23 = calculate_additional_columns(Low_level_checks_ANA23, filter_years, largest_years, diff_years)
    High_level_checks_Pre_day = calculate_additional_columns(High_level_checks_Pre_day, filter_years, largest_years, diff_years)
    low_level_checks_Pre_day = calculate_additional_columns(low_level_checks_Pre_day, filter_years, largest_years, diff_years)

    return High_level_checks_ana23, Low_level_checks_ANA23, High_level_checks_Pre_day, low_level_checks_Pre_day

@st.cache_data(show_spinner=False)
def load_and_preprocess_datasets_for_plotting():
    # Get file paths
    ANA_PATH = get_single_file_from_directory(ANA_DIR)
    PREVIOUS_CURRENT_PATH = get_single_file_from_directory(PREVIOUS_CURRENT_DIR)

    # Load datasets
    ANA23 = pd.read_excel(ANA_PATH, sheet_name="Pre-Change (A)")
    ANA23["Series"] = ANA23["Sector"].astype(str) + ANA23["Transaction"].astype(str) + ANA23["Industry"].astype(str) + ANA23["Product"].astype(str)
    cols = ["Series"] + [col for col in ANA23.columns if col != "Series"]
    ANA23 = ANA23[cols]

    Current = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Post-Change (A)")
    Current["Series"] = Current["Sector"].astype(str) + Current["Transaction"].astype(str) + Current["Industry"].astype(str) + Current["Product"].astype(str)
    cols = ["Series"] + [col for col in Current.columns if col != "Series"]
    Current = Current[cols]

    Previous = pd.read_excel(PREVIOUS_CURRENT_PATH, sheet_name="Pre-Change (A)")
    Previous["Series"] = Previous["Sector"].astype(str) + Previous["Transaction"].astype(str) + Previous["Industry"].astype(str) + Previous["Product"].astype(str)
    cols = ["Series"] + [col for col in Previous.columns if col != "Series"]
    Previous = Previous[cols]

    # Standardize column names
    for df in [ANA23, Current, Previous]:
        df.columns = df.columns.astype(str).str.strip()

    year_columns = [col for col in ANA23.columns if col.isdigit()]

    def clean_numeric_data(df, year_columns):
        numeric_df = df.copy()
        for col in year_columns:
            numeric_df[col] = pd.to_numeric(numeric_df[col], errors="coerce").fillna(0)
        return numeric_df

    ANA23_numeric = clean_numeric_data(ANA23, year_columns)
    Current_numeric = clean_numeric_data(Current, year_columns)
    Previous_numeric = clean_numeric_data(Previous, year_columns)

    def calculate_growth_rates(row, year_columns):
        rates = []
        for i in range(len(year_columns)):
            if i == 0:
                # Base year, no growth
                rates.append(0)
            else:
                prev_val = row[year_columns[i - 1]]
                curr_val = row[year_columns[i]]
                if prev_val != 0:
                    growth = ((curr_val - prev_val) / prev_val) * 100
                else:
                    growth = 0
                rates.append(growth)
        return rates

    ANA23_growth_rates = ANA23_numeric.apply(lambda row: calculate_growth_rates(row, year_columns), axis=1)
    Current_growth_rates = Current_numeric.apply(lambda row: calculate_growth_rates(row, year_columns), axis=1)
    Previous_growth_rates = Previous_numeric.apply(lambda row: calculate_growth_rates(row, year_columns), axis=1)

    # Precompute all combinations for faster filtering
    # Create a dictionary keyed by (Sector, Industry, Product, Transaction)
    precomputed_data = {}
    # We'll iterate once and store all filtered results
    # This ensures that once user changes a filter, we do not recalculate on the fly
    for idx, row in ANA23.iterrows():
        key = (row["Sector"], row["Industry"], row["Product"], row["Transaction"])
        # Filtered rows in each dataset
        filtered_ANA23 = ANA23[(ANA23["Sector"] == key[0]) & (ANA23["Industry"] == key[1]) & (ANA23["Product"] == key[2]) & (ANA23["Transaction"] == key[3])]
        filtered_Current = Current[(Current["Sector"] == key[0]) & (Current["Industry"] == key[1]) & (Current["Product"] == key[2]) & (Current["Transaction"] == key[3])]
        filtered_Previous = Previous[(Previous["Sector"] == key[0]) & (Previous["Industry"] == key[1]) & (Previous["Product"] == key[2]) & (Previous["Transaction"] == key[3])]

        if not filtered_ANA23.empty and not filtered_Current.empty and not filtered_Previous.empty:
            filtered_index = filtered_ANA23.index[0]

            filtered_ANA23_numeric = ANA23_numeric.loc[filtered_index, year_columns]
            filtered_Current_numeric = Current_numeric.loc[filtered_index, year_columns]
            filtered_Previous_numeric = Previous_numeric.loc[filtered_index, year_columns]

            filtered_ANA23_growth = ANA23_growth_rates[filtered_index]
            filtered_Current_growth = Current_growth_rates[filtered_index]
            filtered_Previous_growth = Previous_growth_rates[filtered_index]

            precomputed_data[key] = {
                "ANA23": filtered_ANA23,
                "Current": filtered_Current,
                "Previous": filtered_Previous,
                "ANA23_numeric": filtered_ANA23_numeric,
                "Current_numeric": filtered_Current_numeric,
                "Previous_numeric": filtered_Previous_numeric,
                "ANA23_growth": filtered_ANA23_growth,
                "Current_growth": filtered_Current_growth,
                "Previous_growth": filtered_Previous_growth,
                "year_columns": year_columns
            }

    return ANA23, Current, Previous, ANA23_numeric, Current_numeric, Previous_numeric, ANA23_growth_rates, Current_growth_rates, Previous_growth_rates, year_columns, precomputed_data


st.header("Data Loading")
with st.spinner("Loading data..."):
    # Load all initial data
    (Input_Curr_minus_ANA23, High_level_checks_ana23, Low_level_checks_ANA23,
     Input_Curr_minus_previous, High_level_checks_Pre_day, low_level_checks_Pre_day,
     ANA23, Previous, Current) = load_all_data()

    # Preprocess checks
    (High_level_checks_ana23, Low_level_checks_ANA23,
     High_level_checks_Pre_day, low_level_checks_Pre_day) = preprocess_loaded_data(
        Input_Curr_minus_ANA23, High_level_checks_ana23, Low_level_checks_ANA23,
        Input_Curr_minus_previous, High_level_checks_Pre_day, low_level_checks_Pre_day,
        ANA23, Previous, Current
    )

    # Now load and preprocess datasets for plotting and precomputing all combos
    (ANA23_full, Current_full, Previous_full,
     ANA23_numeric, Current_numeric, Previous_numeric,
     ANA23_growth_rates, Current_growth_rates, Previous_growth_rates,
     year_columns, precomputed_data) = load_and_preprocess_datasets_for_plotting()

st.success("Data loaded and precomputed successfully!")

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

# Sidebar filters
st.sidebar.header("Filter Options")
unique_sectors = ANA23["Sector"].unique()
sector = st.sidebar.selectbox("Select Sector", unique_sectors)
filtered_industries = ANA23[ANA23["Sector"] == sector]["Industry"].unique()
industry = st.sidebar.selectbox("Select Industry", filtered_industries)
filtered_products = ANA23[(ANA23["Sector"] == sector) & (ANA23["Industry"] == industry)]["Product"].unique()
product = st.sidebar.selectbox("Select Product", filtered_products)
filtered_transactions = ANA23[(ANA23["Sector"] == sector) & (ANA23["Industry"] == industry) & (ANA23["Product"] == product)]["Transaction"].unique()
transaction = st.sidebar.selectbox("Select Transaction", filtered_transactions)

key = (sector, industry, product, transaction)

if key not in precomputed_data:
    st.warning("No data available for the selected combination.")
    st.stop()

data = precomputed_data[key]

filtered_ANA23 = data["ANA23"]
filtered_Current = data["Current"]
filtered_Previous = data["Previous"]

filtered_ANA23_numeric = data["ANA23_numeric"]
filtered_Current_numeric = data["Current_numeric"]
filtered_Previous_numeric = data["Previous_numeric"]

filtered_ANA23_growth = data["ANA23_growth"]
filtered_Current_growth = data["Current_growth"]
filtered_Previous_growth = data["Previous_growth"]

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
fig_line.add_trace(go.Scatter(x=data["year_columns"], y=filtered_ANA23_numeric, mode="lines+markers", name="ANA23"))
fig_line.add_trace(go.Scatter(x=data["year_columns"], y=filtered_Current_numeric, mode="lines+markers", name="Current"))
fig_line.add_trace(go.Scatter(x=data["year_columns"], y=filtered_Previous_numeric, mode="lines+markers", name="Previous"))
fig_line.update_layout(title="Yearly Comparison for Levels", xaxis_title="Year", yaxis_title="Values")
st.plotly_chart(fig_line)

# Bar Chart for Growth Rates
st.subheader("Bar Chart: Growth Rates")
fig_bar = go.Figure()
fig_bar.add_trace(go.Bar(x=data["year_columns"], y=filtered_ANA23_growth, name="ANA23 Growth Rate"))
fig_bar.add_trace(go.Bar(x=data["year_columns"], y=filtered_Current_growth, name="Current Growth Rate"))
fig_bar.add_trace(go.Bar(x=data["year_columns"], y=filtered_Previous_growth, name="Previous Growth Rate"))
fig_bar.update_layout(title="Growth Rates Comparison", xaxis_title="Year", yaxis_title="Growth Rate (%)", barmode="group")
st.plotly_chart(fig_bar)
