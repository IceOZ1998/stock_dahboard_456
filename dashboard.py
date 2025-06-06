import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import yfinance as yf
import altair as alt

# === הרשאות מה-SECRET של Streamlit ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === הגדרות כלליות ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"
ceo_name = "Jensen Huang"
ticker = "NVDA"
company_name = "NVIDIA"

# === בחירת תאריכים ===
st.title("📊 Dashboard: השפעת מידע תקשורתי על מחיר מניה")
start_date = st.date_input("תאריך התחלה", datetime(2025, 4, 1))
end_date = st.date_input("תאריך סיום", datetime(2025, 4, 3))

# === כפתור הרצה ===
if st.button("📥 טען נתונים"):

    try:
        # התחברות ל-BigQuery
        client = bigquery.Client(project=project_id)

        # === שליפת נתונים מ-GDELT לפי מנכ"ל ותאריכים ===
        query = f"""
        SELECT
          date,
          AVG(sentiment_score) AS avg_sentiment,
          SUM(numMentions) AS total_mentions,
          AVG(avgSalience) AS avg_salience
        FROM `{project_id}.{dataset}.{table}`
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
            st.warning("לא נמצאו נתונים בטווח שנבחר.")
        else:
            # קטגוריה לפי sentiment
            def label_sentiment(score):
                if score > 0.2:
                    return "😊 חיובי"
                elif score < -0.2:
                    return "☹ שלילי"
                else:
                    return "⏺ נייטרלי"
            df_ceo["sentiment_category"] = df_ceo["avg_sentiment"].apply(label_sentiment)

            st.subheader("📰 טבלת נתונים יומית מה-GDELT")
            st.dataframe(df_ceo)

            # === נתוני מניה מהבורסה
            end_date_yf = end_date + timedelta(days=1)
            df_stock = yf.download(ticker, start=start_date, end=end_date_yf.strftime("%Y-%m-%d"))
            df_stock = df_stock.reset_index()

            if df_stock.empty:
                st.error("❌ לא התקבלו נתוני מניה")
            else:
                # מיזוג נתונים
                df_ceo["date"] = pd.to_datetime(df_ceo["date"])
                df_stock["date"] = pd.to_datetime(df_stock["Date"])
                df_merged = pd.merge(df_ceo, df_stock[["date", "Close"]], on="date", how="inner")
                df_merged.rename(columns={"Close": "stock_price"}, inplace=True)

                # גרף Altair
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

                final_chart = alt.layer(bars, line, labels).resolve_scale(
                    y='independent'
                ).properties(
                    title="📈 קשר בין מידע תקשורתי למחיר מניה",
                    height=300
                )

                st.altair_chart(final_chart, use_container_width=True)

    except Exception as e:
        st.error(f"❌ שגיאה: {e}")
