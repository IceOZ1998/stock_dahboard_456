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

# === MID mapping ===
ceo_data = {
    "Jensen Huang": {"company": "NVIDIA", "ticker": "NVDA", "ceo_mid": "/m/06n774", "company_mid": "/m/09rh_"},
    "Elon Musk": {"company": "Tesla", "ticker": "TSLA", "ceo_mid": "/m/03nzf1", "company_mid": "/m/0dr90d"},
    "Tim Cook": {"company": "Apple", "ticker": "AAPL", "ceo_mid": "/m/05r65m", "company_mid": "/m/0k8z"},
    "Sundar Pichai": {"company": "Alphabet Inc", "ticker": "GOOGL", "ceo_mid": "/m/09gds74", "company_mid": "/g/11bwcf511s"},
    "Satya Nadella": {"company": "Microsoft", "ticker": "MSFT", "ceo_mid": "/m/0q40xjj", "company_mid": "/m/04sv4"},
    "Mark Zuckerberg": {"company": "Meta", "ticker": "META", "ceo_mid": "/m/086dny", "company_mid": "/m/0hmyfsv"},
    "Andy Jassy": {"company": "Amazon", "ticker": "AMZN", "ceo_mid": "/g/11f15hl9r0", "company_mid": "/m/0mgkg"}
}

# === UI Setup ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 Media & Stock Dashboard")

# === UI Selection ===
ceo_options = [f"{ceo} ({data['company']})" for ceo, data in ceo_data.items()]
selected_ceo_display = st.selectbox("Select CEO", ceo_options)
ceo_name = selected_ceo_display.split(" (")[0]
ceo_info = ceo_data[ceo_name]
company_name, ticker = ceo_info["company"], ceo_info["ticker"]
mid_ceo, mid_company = ceo_info["ceo_mid"], ceo_info["company_mid"]

date_range = st.date_input("Select date range", value=(datetime(2025, 4, 1), datetime(2025, 4, 3)))
start_date, end_date = date_range

st.markdown(f"**CEO:** {ceo_name}  |  **Company:** {company_name} ({ticker})")
st.markdown(f"**Date range:** {start_date} to {end_date}")

# === BigQuery info ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_co_articals"

# === Query Function ===
def get_ceo_and_company_stats(project_id, dataset, table, mid_ceo, mid_company, start_date, end_date):
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
      date,
      AVG(sentiment_score) AS avg_sentiment,
      SUM(numMentions) AS total_mentions,
      AVG(avgSalience) AS avg_salience
    FROM `{project_id}.{dataset}.{table}`
    WHERE mid IN (@mid_ceo, @mid_company)
      AND date BETWEEN @start_date AND @end_date
    GROUP BY date
    ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mid_ceo", "STRING", mid_ceo),
            bigquery.ScalarQueryParameter("mid_company", "STRING", mid_company),
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
    df["salience_label"] = "avgSalience: " + df["avg_salience"].round(3).astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
    return df

# === Main Button Action ===
if st.button("🔍 Run Analysis"):
    df_stock = yf.download(ticker, start=start_date, end=end_date + pd.Timedelta(days=1))

    if df_stock.empty:
        st.warning("⚠️ No stock data found.")
    else:
        start_price = df_stock["Close"].iloc[0]
        end_price = df_stock["Close"].iloc[-1]
        trend = "📈 Up" if end_price > start_price else "📉 Down" if end_price < start_price else "➖ No change"

    try:
        df_ceo = get_ceo_and_company_stats(
            project_id, dataset, table, mid_ceo, mid_company, start_date, end_date
        )
    except Exception as e:
        st.error(f"❌ Error retrieving data from BigQuery: {e}")
        st.stop()

    if df_ceo.empty:
        st.warning("⚠️ No media data found for this CEO/company.")
    else:
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("💰 Stock Closing Price")
            df_stock.index = df_stock.index.date
            st.line_chart(df_stock["Close"])
            st.markdown(f"**Overall price trend:** {trend} (from {start_price:.2f} to {end_price:.2f})")

        with col2:
            st.subheader("📰 Daily Media Mentions")

            base = alt.Chart(df_ceo).encode(x=alt.X("date:N", title="Date", axis=alt.Axis(labelAngle=0)))
            bars = base.mark_bar(color="orange").encode(
                y=alt.Y("total_mentions:Q", title="Mentions"),
                tooltip=["date", "total_mentions"]
            )
            salience_labels = base.mark_text(dy=-15, fontSize=10, color="black").encode(
                y="total_mentions:Q",
                text="salience_label"
            )
            sentiment_labels = base.mark_text(dy=-30, fontSize=12, fontWeight="bold", color="gray").encode(
                y="total_mentions:Q",
                text="sentiment_category"
            )

            combined_chart = alt.layer(bars, salience_labels, sentiment_labels).properties(
                height=300,
                width=400,
                title="📢 Mentions per Day (with Sentiment & Salience)"
            )

            st.altair_chart(combined_chart, use_container_width=False)

        st.markdown("### 📄 Raw GDELT Data")
        st.dataframe(df_ceo.style.format({
            "avg_sentiment": "{:.2f}",
            "avg_salience": "{:.2f}"
        }))
