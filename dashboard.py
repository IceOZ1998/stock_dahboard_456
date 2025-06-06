import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import yfinance as yf
import altair as alt

# === Load credentials from Streamlit secrets ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === CEO ↔ Company ↔ Ticker Mapping ===
ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Andy Jassy": ("Amazon", "AMZN")
}

# === UI Setup ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 Media Coverage Impact on Stock Price")

selected_ceo = st.selectbox("Select CEO", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[selected_ceo]
ceo_name = selected_ceo

start_date = st.date_input("Start date", datetime(2025, 4, 1))
end_date = st.date_input("End date", datetime(2025, 4, 3))

if st.button("📥 Load Data"):

    try:
        # === BigQuery query ===
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
            st.warning("⚠️ No GDELT data found for this CEO in the selected date range.")
            st.stop()

        # === Download stock data
        end_date_yf = end_date + timedelta(days=1)
        df_stock = yf.download(ticker, start=start_date, end=end_date_yf.strftime("%Y-%m-%d"))
        df_stock = df_stock.reset_index()

        if "Date" in df_stock.columns:
            df_stock.rename(columns={"Date": "date"}, inplace=True)
        df_stock["date"] = pd.to_datetime(df_stock["date"])
        df_stock.columns = [col.lower() for col in df_stock.columns]  # lowercase everything

        # === Prepare & merge
        df_ceo["date"] = pd.to_datetime(df_ceo["date"])
        df_merged = pd.merge(df_ceo, df_stock[["date", "close"]], on="date", how="inner")
        df_merged.rename(columns={"close": "stock_price"}, inplace=True)

        # === Label sentiment and salience
        def label_sentiment(score):
            if score > 0.2:
                return "Positive"
            elif score < -0.2:
                return "Negative"
            else:
                return "Neutral"

        df_merged["sentiment_category"] = df_merged["avg_sentiment"].apply(label_sentiment)
        df_merged["salience_label"] = df_merged["avg_salience"].round(3).astype(str)

        # === Altair Chart ===
        base = alt.Chart(df_merged).encode(x=alt.X("date:T", title="Date"))

        line = base.mark_line(color="steelblue").encode(
            y=alt.Y("stock_price:Q", title="Stock Price"),
            tooltip=["date", "stock_price"]
        )

        bars = base.mark_bar(color="orange", opacity=0.7).encode(
            y=alt.Y("total_mentions:Q", title="Mentions"),
            tooltip=["total_mentions"]
        )

        salience_labels = base.mark_text(
            align="center", baseline="bottom", dy=-15, fontSize=10, color="black"
        ).encode(
            y="total_mentions:Q",
            text="salience_label"
        )

        sentiment_labels = base.mark_text(
            align="center", baseline="bottom", dy=-30, fontSize=11, fontWeight="bold", color="gray"
        ).encode(
            y="total_mentions:Q",
            text="sentiment_category"
        )

        final_chart = alt.layer(bars, line, salience_labels, sentiment_labels).resolve_scale(
            y='independent'
        ).properties(
            title=f"{company_name}: Stock Price vs Mentions & Sentiment",
            height=350
        )

        st.altair_chart(final_chart, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Error: {e}")
