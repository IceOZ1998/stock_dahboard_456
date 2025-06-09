import os
import json
import streamlit as st
from datetime import datetime
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import altair as alt

# === Page Config ===
st.set_page_config(page_title="EchoMarket - Media & Stock Dashboard", layout="wide")

# === Dark Mode Toggle ===
dark_mode = st.sidebar.toggle("🌙 Dark Mode", value=False)

if dark_mode:
    st.markdown(
        """
        <style>
            body {
                background-color: #1e1e1e;
                color: #ffffff;
            }
            .stApp {
                background-color: #1e1e1e;
                color: #ffffff;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

# === Title ===
st.title("📊 EchoMarket - Media & Stock Dashboard")

# === Load Google service account key from Streamlit secrets ===
with open("/tmp/service_account.json", "w") as f:
    f.write(st.secrets["google_service_account"]["json"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/service_account.json"

# === CEO to Company Mapping (includes MIDs) ===
ceo_mapping = {
    "Jensen Huang": {"company": "NVIDIA", "ticker": "NVDA", "ceo_mid": "/m/06n774", "org_mid": "/m/09rh_"},
    "Elon Musk": {"company": "Tesla", "ticker": "TSLA", "ceo_mid": "/m/03nzf1", "org_mid": "/m/0dr90d"},
    "Tim Cook": {"company": "Apple", "ticker": "AAPL", "ceo_mid": "/m/05r65m", "org_mid": "/m/0k8z"},
    "Sundar Pichai": {"company": "Alphabet", "ticker": "GOOGL", "ceo_mid": "/m/09gds74", "org_mid": "/g/11bwcf511s"},
    "Satya Nadella": {"company": "Microsoft", "ticker": "MSFT", "ceo_mid": "/m/0q40xjj", "org_mid": "/m/04sv4"},
    "Mark Zuckerberg": {"company": "Meta", "ticker": "META", "ceo_mid": "/m/086dny", "org_mid": "/m/0hmyfsv"},
    "Andy Jassy": {"company": "Amazon", "ticker": "AMZN", "ceo_mid": "/g/11f15hl9r0", "org_mid": "/m/0mgkg"}
}

# === UI: Select CEO and Dates ===
ceo_options = [f"{ceo} ({data['company']})" for ceo, data in ceo_mapping.items()]
selected_ceo_display = st.selectbox("Select CEO", ceo_options)
ceo_name = selected_ceo_display.split(" (")[0]
company_info = ceo_mapping[ceo_name]
company_name, ticker = company_info["company"], company_info["ticker"]
ceo_mid, org_mid = company_info["ceo_mid"], company_info["org_mid"]

# === Require Date Input ===
date_range = st.date_input("Select date range")
if not date_range or len(date_range) != 2:
    st.warning("⏳ Please select a valid start and end date to continue.")
    st.stop()

start_date, end_date = date_range

st.markdown(f"**CEO:** {ceo_name}  |  **Company:** {company_name} ({ticker})")
st.markdown(f"**Date range:** {start_date} to {end_date}")

# === GDELT BigQuery table ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_extended"

def get_daily_stats(project_id, dataset, table, mids, start_date, end_date):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
      date,
      AVG(sentiment_score) AS avg_sentiment,
      SUM(numMentions) AS total_mentions,
      COUNT(DISTINCT url) AS total_articles,
      AVG(avgSalience) AS avg_salience
    FROM `{project_id}.{dataset}.{table}`
    WHERE mid IN UNNEST(@mid_list)
      AND date BETWEEN @start_date AND @end_date
    GROUP BY date
    ORDER BY date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("mid_list", "STRING", mids),
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
        elif score > 0:
            return "Slightly Positive"
        elif score >= -0.2:
            return "Slightly Negative"
        else:
            return "Negative"

    df["sentiment_category"] = df["avg_sentiment"].apply(classify_sentiment)
    df["salience_label"] = "avgSalience: " + df["avg_salience"].round(3).astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
    return df

def sentiment_label(score):
    if pd.isna(score):
        return "Neutral"
    elif score > 0.2:
        return "Positive"
    elif score > 0:
        return "Slightly Positive"
    elif score >= -0.2:
        return "Slightly Negative"
    else:
        return "Negative"

# === Run Analysis ===
if st.button("🔍 Run Analysis"):
    df_stock = yf.download(ticker, start=start_date, end=end_date + pd.Timedelta(days=1))
    if df_stock.empty:
        st.warning("⚠️ No stock data found for the selected date range.")
    else:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()
        trend = "📈 Up" if end_price > start_price else "📉 Down" if end_price < start_price else "➖ No change"

    try:
        df_ceo = get_daily_stats(project_id, dataset, table, [ceo_mid, org_mid], start_date, end_date)
    except Exception as e:
        st.error(f"❌ Error retrieving data from BigQuery: {e}")
        st.stop()

    if df_ceo.empty:
        st.warning("⚠️ No media data found for this CEO or company in the selected range.")
    else:
        st.markdown(f"**Overall price trend:** {trend} (from {start_price:.2f} to {end_price:.2f})")

        avg_articles = df_ceo["total_articles"].mean()
        avg_sentiment = df_ceo["avg_sentiment"].mean()
        sentiment_tag = sentiment_label(avg_sentiment)
        has_significant_coverage = avg_articles >= 5

        if trend == "📈 Up":
            if sentiment_tag in ["Positive", "Slightly Positive"] and has_significant_coverage:
                conclusion = "The stock is trending up with positive sentiment and significant media coverage."
            elif sentiment_tag in ["Positive", "Slightly Positive"]:
                conclusion = "The stock is trending up with positive sentiment, but media coverage is relatively low."
            elif not has_significant_coverage:
                conclusion = "The stock is trending up, but sentiment and media coverage are not particularly strong."
            else:
                conclusion = "The stock is trending up but sentiment does not fully support this trend."
        elif trend == "📉 Down":
            if sentiment_tag in ["Negative", "Slightly Negative"] and has_significant_coverage:
                conclusion = "The stock is trending down with negative sentiment and significant media coverage."
            elif sentiment_tag in ["Negative", "Slightly Negative"]:
                conclusion = "The stock is trending down with negative sentiment, but media coverage is relatively low."
            elif not has_significant_coverage:
                conclusion = "The stock is trending down, but sentiment and media coverage are not particularly strong."
            else:
                conclusion = "The stock is trending down but sentiment does not fully support this trend."
        else:
            conclusion = "There is no significant price change, but media coverage is notable." if has_significant_coverage else "There is no significant price change or media coverage."

        st.markdown("### 📝 Correlation Conclusion:")
        st.markdown(conclusion)

        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("💰 Stock Closing Price")
            df_stock.index = df_stock.index.date
            st.line_chart(df_stock["Close"])

        with col2:
            st.subheader("📰 Daily Media Mentions")
            base = alt.Chart(df_ceo).encode(x=alt.X("date:N", title="Date", axis=alt.Axis(labelAngle=0)))
            bars = base.mark_bar(color="orange").encode(
                y=alt.Y("total_articles:Q", title="Articles"),
                tooltip=["date", "total_articles"]
            )
            salience_labels = base.mark_text(dy=-15, fontSize=10, color="black").encode(
                y="total_articles:Q", text="salience_label"
            )
            sentiment_labels = base.mark_text(dy=-30, fontSize=12, fontWeight="bold", color="gray").encode(
                y="total_articles:Q", text="sentiment_category"
            )
            combined_chart = alt.layer(bars, salience_labels, sentiment_labels).properties(
                height=300, width=400, title="📢 Articles per Day (with Sentiment & Salience)"
            )
            st.altair_chart(combined_chart, use_container_width=False)

        st.markdown("### 📄 Raw GDELT Data")
        st.dataframe(df_ceo.style.format({
            "avg_sentiment": "{:.2f}",
            "avg_salience": "{:.2f}"
        }))
