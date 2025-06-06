# dashboard_app.py

import streamlit as st
from google.cloud import bigquery
import pandas as pd
import yfinance as yf
from datetime import timedelta

# הגדרת מידע על מנכ"לים
ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Mark Zuckerberg": ("Meta", "META"),
    "Andy Jassy": ("Amazon", "AMZN")
}

project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"

# שליפת טווח תאריכים לפי שם מנכ"ל
def get_date_range(ceo_name):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT MIN(date) AS start_date, MAX(date) AS end_date
    FROM `{project_id}.{dataset}.{table}`
    WHERE name = "{ceo_name}"
    """
    df = client.query(query).result().to_dataframe()
    if df.empty or df.isnull().values.any():
        return None, None
    return df["start_date"].iloc[0], pd.to_datetime(df["end_date"].iloc[0]) + timedelta(days=1)

# שליפת נתוני מניה
def get_stock_data(ticker, start_date, end_date):
    return yf.download(ticker, start=start_date, end=end_date.strftime("%Y-%m-%d"))

# ממשק Streamlit
st.title("📊 Media & Stock Dashboard")

ceo_name = st.selectbox("בחר מנכ\"ל", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[ceo_name]

start_date, end_date = get_date_range(ceo_name)

if start_date is None:
    st.error("לא נמצאו תאריכים עבור המנכ\"ל")
else:
    df_stock = get_stock_data(ticker, start_date, end_date)
    if df_stock.empty:
        st.warning("לא נמצאו נתונים ב-Yahoo Finance לטווח הנתון")
    else:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()
        trend = "📈 עלייה" if end_price > start_price else "📉 ירידה" if end_price < start_price else "➖ ללא שינוי"

        st.subheader(f"{company_name} ({ticker})")
        st.write(f"🗓️ טווח תאריכים: {start_date.date()} עד {end_date.date()}")
        st.line_chart(df_stock["Close"])
        st.markdown(f"**תנועת מחיר כוללת:** {trend} (מ־{start_price:.2f} ל־{end_price:.2f})")
