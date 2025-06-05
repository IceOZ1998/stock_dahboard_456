import os
import json

# שמירת מפתח ההרשאה כקובץ זמני
with open("/tmp/service_account.json", "w") as f:
    json.dump(st.secrets["google_service_account"], f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"



import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import yfinance as yf
from datetime import timedelta

# === Metadata ===
st.set_page_config(page_title="Market Echo", layout="wide")
st.title("Market Echo: Media Sentiment and Stock Performance")
st.markdown("Analyze how CEO-related media coverage echoes through the market.")

# === Project definitions ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"

ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Mark Zuckerberg": ("Meta", "META"),
    "Andy Jassy": ("Amazon", "AMZN")
}

# === Sidebar: CEO selection ===
ceo_name = st.sidebar.selectbox("Choose a CEO:", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[ceo_name]

# === Connect to BigQuery ===
client = bigquery.Client(project=project_id)

# === Get full date range ===
query = f"""
SELECT MIN(date) AS start_date, MAX(date) AS end_date
FROM `{project_id}.{dataset}.{table}`
WHERE name = "{ceo_name}"
"""
df_dates = client.query(query).result().to_dataframe()

if df_dates.empty or df_dates.isnull().values.any():
    st.error("No date range found for the selected CEO.")
else:
    default_start = df_dates["start_date"].iloc[0]
    default_end = df_dates["end_date"].iloc[0]

    # === Date selection ===
    start_date = st.sidebar.date_input("Start date", value=default_start, min_value=default_start, max_value=default_end)
    end_date = st.sidebar.date_input("End date", value=default_end, min_value=default_start, max_value=default_end)

    if start_date >= end_date:
        st.warning("Start date must be earlier than end date.")
    else:
        end_date_plus = end_date + timedelta(days=1)
        df_stock = yf.download(ticker, start=start_date, end=end_date_plus)

        if not df_stock.empty and len(df_stock) >= 2:
            start_price = df_stock["Close"].iloc[0].item()
            end_price = df_stock["Close"].iloc[-1].item()

            if end_price > start_price:
                trend = "\ud83d\udcc8 Increase"
            elif end_price < start_price:
                trend = "\ud83d\udcc9 Decrease"
            else:
                trend = "\u2796 No change"

            st.subheader("Price Summary")
            st.markdown(f"**CEO:** {ceo_name} ({company_name})")
            st.markdown(f"**Date Range:** {start_date} to {end_date}")
            st.dataframe(df_stock[["Close"]])
            st.markdown(f"**\U0001F4CA Overall trend:** {trend} (from {round(start_price, 2)} to {round(end_price, 2)})")
        else:
            st.warning("No stock data retrieved.")
