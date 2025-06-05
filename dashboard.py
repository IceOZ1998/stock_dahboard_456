import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import yfinance as yf
from datetime import timedelta

# === Metadata ===
st.set_page_config(page_title="Market Echo", layout="wide")
st.title("\ud83d\udd0a Market Echo: Media Sentiment and Stock Performance")
st.markdown("Analyze how CEO-related media coverage echoes through the market.")

# === Project definitions ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"

ceo_to_company = {
    "Jensen Huang": ("NVIDIA", "NVDA"),
    "Elon Musk": ("Tesla", "TSLA"),
    "Tim Cook": ("Apple", "AAPL"),
    "Sundar Pichai": ("Alphabet", "GOOGL"),
    "Satya Nadella": ("Microsoft", "MSFT"),
    "Mark Zuckerberg": ("Meta", "META"),
    "Andy Jassy": ("Amazon", "AMZN")
}

# === Sidebar: CEO selection ===
ceo_name = st.sidebar.selectbox("Choose a CEO:", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[ceo_name]

# === Connect to BigQuery ===
client = bigquery.Client(project=project_id)

# === Get date range ===
query = f"""
SELECT
  MIN(date) AS start_date,
  MAX(date) AS end_date
FROM
  `{project_id}.{dataset}.{table}`
WHERE
  name = "{ceo_name}"
"""
df_dates = client.query(query).result().to_dataframe()

if df_dates.empty or df_dates.isnull().values.any():
    st.error("No date range found for the selected CEO.")
else:
    start_date = df_dates["start_date"].iloc[0]
    end_date = pd.to_datetime(df_dates["end_date"].iloc[0]) + timedelta(days=1)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    end_date_str_sql = (end_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # === Stock data ===
    df_stock = yf.download(ticker, start=start_date_str, end=end_date_str)

    # === GDELT summary query ===
    summary_query = f"""
    SELECT
      DATE,
      SUM(numMentions) AS numMentions,
      ROUND(AVG(avgSalience), 3) AS avgSalience,
      ROUND(AVG(sentiment_score), 3) AS sentiment_score
    FROM
      `{project_id}.{dataset}.{table}`
    WHERE
      name = "{ceo_name}"
      AND DATE BETWEEN "{start_date_str}" AND "{end_date_str_sql}"
    GROUP BY DATE
    ORDER BY DATE
    """
    df_summary = client.query(summary_query).result().to_dataframe()

    # === Sentiment label ===
    def classify_sentiment(score):
        if pd.isna(score):
            return "Not available"
        elif score > 0.2:
            return "Positive"
        elif score < -0.2:
            return "Negative"
        else:
            return "Neutral"

    df_summary["sentiment_label"] = df_summary["sentiment_score"].apply(classify_sentiment)

    # === Display daily summaries ===
    st.subheader("Daily Summary")
    for idx, row in df_summary.iterrows():
        date_str = pd.to_datetime(row["DATE"]).strftime("%d.%m.%y")
        st.markdown(f"**{date_str}** - numMentions: {int(row['numMentions'])}; avgSalience: {row['avgSalience']}; sentiment: {row['sentiment_label']}")

    # === Display stock data table ===
    if not df_stock.empty and len(df_stock) >= 2:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()

        if end_price > start_price:
            trend = "\ud83d\udcc8 Increase"
        elif end_price < start_price:
            trend = "\ud83d\udcc9 Decrease"
        else:
            trend = "\u2796 No change"

        st.subheader("Stock Price")
        st.dataframe(df_stock[["Close"]])
        st.markdown(f"**\U0001F4CA Overall trend:** {trend} (from {round(start_price, 2)} to {round(end_price, 2)})")
    else:
        st.warning("No stock data retrieved.")
