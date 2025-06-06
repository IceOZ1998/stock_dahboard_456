import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import yfinance as yf
import altair as alt

# === הרשאות מ-Stremlit Secrets ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === מיפוי מנכ"לים וחברות ===
ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Andy Jassy": ("Amazon", "AMZN")
}

# === ממשק משתמש ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 תקשורת ומניה – השפעה יומית")

selected_ceo = st.selectbox("בחר מנכ\"ל", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[selected_ceo]
ceo_name = selected_ceo

start_date = st.date_input("תאריך התחלה", datetime(2025, 4, 1))
end_date = st.date_input("תאריך סיום", datetime(2025, 4, 3))

# === כפתור טעינה ===
if st.button("📥 טען נתונים"):

    try:
        # === שליפת נתוני BigQuery ===
        client = bigquery.Client()

        query = f"""
        SELECT
          date,
          AVG(sentiment_score) AS avg_sentiment,
          SUM(numMentions) AS total_mentions,
          AVG(avgSalience) AS avg_salience
        FROM `bigdata456.Big_Data_456_data.ceo_articles_nvidia_test`
        WHERE name = @ceo_name
          AND date BETWEEN @start_date AND @end_date
        GROUP BY date
        ORDER BY date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ceo_name", "STRING", ceo_name),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date.strftime("%Y-%m-%d")),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date.strftime("%Y-%m-%d")),
            ]
        )

        df_ceo = client.query(query, job_config=job_config).result().to_dataframe()

        if df_ceo.empty:
            st.warning("⚠️ לא נמצאו נתונים ב-GDELT לטווח הזה.")
            st.stop()

        # תיוג סנטימנט
        def label_sentiment(score):
            if score > 0.2:
                return "😊 חיובי"
            elif score < -0.2:
                return "☹ שלילי"
            else:
                return "⏺ נייטרלי"

        df_ceo["sentiment_category"] = df_ceo["avg_sentiment"].apply(label_sentiment)
        st.subheader("📰 טבלת נתונים יומית (GDELT)")
        st.dataframe(df_ceo)

        # === נתוני מניה מ־yfinance ===
        end_date_yf = end_date + timedelta(days=1)
        df_stock = yf.download(ticker, start=start_date, end=end_date_yf.strftime("%Y-%m-%d"))
        df_stock = df_stock.reset_index(level=0)  # 🔧 תיקון ה־merge

        if df_stock.empty:
            st.error("❌ לא נמצאו נתוני מניה")
            st.stop()

        # === מיזוג נתונים על בסיס תאריך ===
        df_ceo["date"] = pd.to_datetime(df_ceo["date"])
        df_stock["date"] = pd.to_datetime(df_stock["Date"])
        df_merged = pd.merge(df_ceo, df_stock[["date", "Close"]], on="date", how="inner")
        df_merged.rename(columns={"Close": "stock_price"}, inplace=True)

        # === גרף משולב: מניה + אזכורים + סנטימנט ===
        line = alt.Chart(df_merged).mark_line(color="steelblue").encode(
            x=alt.X("date:T", title="תאריך"),
            y=alt.Y("stock_price:Q", title="מחיר מניה", scale=alt.Scale(zero=False)),
            tooltip=["date", "stock_price"]
        )

        bars = alt.Chart(df_merged).mark_bar(opacity=0.6, color="orange").encode(
            x="date:T",
            y=alt.Y("total_mentions:Q", title="כמות אזכורים", axis=alt.Axis(titleColor="orange")),
            tooltip=["total_mentions"]
        )

        labels = alt.Chart(df_merged).mark_text(
            align="center",
            baseline="bottom",
            dy=-5,
            fontSize=12
        ).encode(
            x="date:T",
            y="total_mentions:Q",
            text="sentiment_category"
        )

        chart = alt.layer(bars, line, labels).resolve_scale(
            y='independent'
        ).properties(
            title=f"📈 מניה מול תקשורת ({company_name})",
            height=300
        )

        st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"❌ שגיאה: {e}")
