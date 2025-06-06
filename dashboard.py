import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import yfinance as yf
import altair as alt

# === Load service account credentials from Streamlit secrets ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === CEO → Company → Ticker mapping ===
ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Andy Jassy": ("Amazon", "AMZN")
}

# === UI ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 Media Coverage Impact on Stock Price")

selected_ceo = st.selectbox("Select CEO", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[selected_ceo]
ceo_name = selected_ceo

start_date = st.date_input("Start date", datetime(2025, 4, 1))
end_date = st.date_input("End date", datetime(2025, 4, 3))

# === Load button ===
if st.button("📥 Load Data"):

    try:
        # === BigQuery Query ===
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
            st.warning("⚠️ No GDELT data found for this date range.")
            st.stop()

        def label_sentiment(score):
            if score > 0.2:
                return "😊 Positive"
            elif score < -0.2:
                return "☹ Negative"
            else:
                return "⏺ Neutral"

        df_ceo["sentiment_category"] = df_ceo["avg_sentiment"].apply(label_sentiment)
        st.subheader("📄 GDELT Daily Summary")
        st.dataframe(df_ceo)

        # === Stock data from yfinance ===
        end_date_yf = end_date + timedelta(days=1)
        df_stock = yf.download(ticker, start=start_date, end=end_date_yf.strftime("%Y-%m-%d"))
        df_stock = df_stock.reset_index()
        df_stock.columns = df_stock.columns.map(str)
        df_stock = df_stock.rename(columns=lambda x: x.lower())  # fixes 'Date' issue

        if df_stock.empty:
            st.error("❌ No stock data retrieved.")
            st.stop()

        df_ceo["date"] = pd.to_datetime(df_ceo["date"])
        df_stock["date"] = pd.to_datetime(df_stock["date"])
        df_merged = pd.merge(df_ceo, df_stock[["date", "close"]], on="date", how="inner")
        df_merged.rename(columns={"close": "stock_price"}, inplace=True)

        # === Combined chart ===
        line = alt.Chart(df_merged).mark_line(color="steelblue").encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("stock_price:Q", title="Stock Price", scale=alt.Scale(zero=False)),
            tooltip=["date", "stock_price"]
        )

        bars = alt.Chart(df_merged).mark_bar(opacity=0.6, color="orange").encode(
            x="date:T",
            y=alt.Y("total_mentions:Q", title="Mentions", axis=alt.Axis(titleColor="orange")),
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
            title=f"📈 {company_name}: Stock Price vs Media Activity",
            height=300
        )

        st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Error: {e}")
