import os
import json
import streamlit as st
from datetime import datetime
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import altair as alt

# === Load Google service account key from Streamlit secrets ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === Basic setup ===
ceo_name = "Jensen Huang"
company_name, ticker = "NVIDIA", "NVDA"
start_date = datetime(2025, 4, 1)
end_date = datetime(2025, 4, 3)
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"

# === Dashboard layout ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 Media & Stock Dashboard – Jensen Huang")
st.markdown(f"**CEO:** {ceo_name}  |  **Company:** {company_name} ({ticker})")
st.markdown(f"**Date range:** {start_date.date()} to {end_date.date()}")

# === Query GDELT data from BigQuery ===
def get_ceo_daily_stats(project_id, dataset, table, ceo_name, start_date, end_date):
    client = bigquery.Client(project=project_id)

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
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    df = client.query(query, job_config=job_config).result().to_dataframe()

    def classify_sentiment(score):
        if pd.isna(score):
            return "Neutral"
        elif score > 0.2:
            return "Positive"
        elif score < -0.2:
            return "Negative"
        else:
            return "Neutral"

    df["sentiment_category"] = df["avg_sentiment"].apply(classify_sentiment)
    df["avg_salience_label"] = df["avg_salience"].round(3).astype(str)
    return df

# === Main action ===
if st.button("🔍 Run Analysis"):
    # === Load stock data ===
    df_stock = yf.download(ticker, start=start_date, end=end_date + pd.Timedelta(days=1))

    if df_stock.empty:
        st.warning("⚠️ No stock data found for the selected date range.")
    else:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()
        trend = "📈 Up" if end_price > start_price else "📉 Down" if end_price < start_price else "➖ No change"

        st.subheader("💰 Stock Closing Price")
        st.line_chart(df_stock["Close"])
        st.markdown(f"**Overall price trend:** {trend} (from {start_price:.2f} to {end_price:.2f})")

    # === Load GDELT data ===
    try:
        df_ceo = get_ceo_daily_stats(
            project_id=project_id,
            dataset=dataset,
            table=table,
            ceo_name=ceo_name,
            start_date=start_date.date(),
            end_date=end_date.date()
        )
    except Exception as e:
        st.error(f"❌ Error retrieving data from BigQuery: {e}")
        st.stop()

    if df_ceo.empty:
        st.warning("⚠️ No media data found for this CEO in the selected range.")
    else:
        st.subheader("📰 Daily Media Analysis (GDELT)")
        st.dataframe(df_ceo.style.format({
            "avg_sentiment": "{:.2f}",
            "avg_salience": "{:.2f}"
        }))

        # === Single chart with mentions, salience, sentiment ===
        base = alt.Chart(df_ceo).encode(x=alt.X("date:T", title="Date"))

        bars = base.mark_bar(color="orange").encode(
            y=alt.Y("total_mentions:Q", title="Mentions"),
            tooltip=["date", "total_mentions"]
        )

        salience_labels = base.mark_text(
            dy=-15,
            fontSize=11,
            color="black"
        ).encode(
            y="total_mentions:Q",
            text="avg_salience_label"
        )

        sentiment_labels = base.mark_text(
            dy=-30,
            fontSize=12,
            fontWeight="bold",
            color="gray"
        ).encode(
            y="total_mentions:Q",
            text="sentiment_category"
        )

        combined_chart = alt.layer(bars, salience_labels, sentiment_labels).properties(
            title="📢 Mentions per Day (with Sentiment & Salience)",
            height=350
        )

        st.altair_chart(combined_chart, use_container_width=True)
