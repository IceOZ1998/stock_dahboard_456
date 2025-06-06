import streamlit as st
from google.cloud import bigquery
import pandas as pd
import yfinance as yf
from datetime import date

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

st.set_page_config(page_title="CEO Media & Stocks", layout="wide")
st.title("📊 Media & Stock Dashboard")

# בחירת מנכ"ל
ceo_name = st.selectbox("בחר מנכ\"ל", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[ceo_name]

# בחירת טווח תאריכים
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("תאריך התחלה", date(2024, 12, 1))
with col2:
    end_date = st.date_input("תאריך סיום", date(2025, 3, 31))

if start_date >= end_date:
    st.error("❗ תאריך הסיום חייב להיות אחרי תאריך ההתחלה")
else:
    # שליפת נתוני מניה
    df_stock = yf.download(ticker, start=start_date, end=end_date.strftime("%Y-%m-%d"))

    if df_stock.empty:
        st.warning("לא נמצאו נתוני מניה בטווח הנבחר")
    else:
        start_price = df_stock["Close"].iloc[0]
        end_price = df_stock["Close"].iloc[-1]
        trend = "📈 עלייה" if end_price > start_price else "📉 ירידה" if end_price < start_price else "➖ ללא שינוי"

        st.subheader(f"{company_name} ({ticker})")
        st.write(f"🗓️ טווח תאריכים: {start_date} עד {end_date}")
        st.line_chart(df_stock["Close"])
        st.markdown(f"**תנועת מחיר כוללת:** {trend} (מ־{start_price:.2f} ל־{end_price:.2f})")
