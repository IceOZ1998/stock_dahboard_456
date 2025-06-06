import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime

# ניסיון ראשוני רק עבור Jensen Huang
ceo_name = "Jensen Huang"
company_name, ticker = "NVIDIA", "NVDA"
start_date = datetime(2025, 4, 1)
end_date = datetime(2025, 4, 3)

st.set_page_config(page_title="Dashboard - ניסוי", layout="wide")
st.title("📊 Media & Stock Dashboard (ניסוי ראשוני)")

st.markdown(f"**מנכ\"ל:** {ceo_name}  |  **חברה:** {company_name} ({ticker})")
st.markdown(f"**טווח תאריכים:** {start_date.date()} עד {end_date.date()}")

# כפתור להרצת הניתוח
if st.button("🔍 הפעל ניתוח"):
    # שליפת נתוני מניה
    df_stock = yf.download(ticker, start=start_date, end=end_date + pd.Timedelta(days=1))

    if df_stock.empty:
        st.warning("⚠️ לא נמצאו נתונים בטווח שנבחר")
    else:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()
        trend = "📈 עלייה" if end_price > start_price else "📉 ירידה" if end_price < start_price else "➖ ללא שינוי"

        st.subheader("📈 גרף סגירה יומית")
        st.line_chart(df_stock["Close"])

        st.markdown(f"**תנועת מחיר כוללת:** {trend} (מ־{start_price:.2f} ל־{end_price:.2f})")
