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

# === Dashboard layout ===
st.set_page_config(page_title="Media & Stock Dashboard", layout="wide")
st.title("📊 Media & Stock Dashboard")

# === UI: Select CEO and Dates ===
ceo_options = [f"{ceo} ({data['company']})" for ceo, data in ceo_mapping.items()]
selected_ceo_display = st.selectbox("Select CEO", ceo_options)
ceo_name = selected_ceo_display.split(" (")[0]
company_info = ceo_mapping[ceo_name]
company_name, ticker = company_info["company"], company_info["ticker"]
ceo_mid, org_mid = company_info["ceo_mid"], company_info["org_mid"]

date_range = st.date_input("Select date range", value=(datetime(2025, 4, 1), datetime(2025, 4, 3)))
start_date, end_date = date_range

# === Display selected inputs ===
st.markdown(f"**CEO:** {ceo_name}  |  **Company:** {company_name} ({ticker})")
st.markdown(f"**Date range:** {start_date} to {end_date}")

# === GDELT BigQuery table ===
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_extended"

# === Query GDELT data from BigQuery ===
def get_daily_stats(project_id, dataset, table, mids, start_date, end_date):
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
      date,
      AVG(sentiment_score) AS avg_sentiment,
      SUM(numMentions) AS total_mentions,
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
        elif score < -0.2:
            return "Negative"
        else:
            return "Neutral"

    df["sentiment_category"] = df["avg_sentiment"].apply(classify_sentiment)
    df["salience_label"] = "avgSalience: " + df["avg_salience"].round(3).astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
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

    # === Load GDELT data ===
    try:
        df_ceo = get_daily_stats(
            project_id=project_id,
            dataset=dataset,
            table=table,
            mids=[ceo_mid, org_mid],
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        st.error(f"❌ Error retrieving data from BigQuery: {e}")
        st.stop()

    if df_ceo.empty:
        st.warning("⚠️ No media data found for this CEO or company in the selected range.")
    else:
        # ====== Correlation analysis block ======
        df_stock_reset = df_stock.reset_index()
        df_stock_reset["date"] = df_stock_reset["Date"].dt.strftime('%Y-%m-%d')

        df_merge = pd.merge(df_stock_reset, df_ceo, on="date", how="inner")

        corr_price_mentions = df_merge["Close"].corr(df_merge["total_mentions"])
        corr_price_sentiment = df_merge["Close"].corr(df_merge["avg_sentiment"])

        def correlation_conclusion(corr, var_name):
            threshold = 0.3
            if corr >= threshold:
                return f"קיים קשר חיובי משמעותי בין מחיר המניה ל-{var_name}. כלומר, עלייה ב-{var_name} נוטה להתלוות לעלייה במחיר."
            elif corr <= -threshold:
                return f"קיים קשר שלילי משמעותי בין מחיר המניה ל-{var_name}. כלומר, עלייה ב-{var_name} נוטה להתלוות לירידה במחיר."
            else:
                return f"לא נמצא קשר ליניארי משמעותי בין מחיר המניה ל-{var_name}."

        conclusion_mentions = correlation_conclusion(corr_price_mentions, "כמות הכתבות")
        conclusion_sentiment = correlation_conclusion(corr_price_sentiment, "הסנטימנט הממוצע")

        # ====== Display correlation results ======
        st.markdown("### 📊 ניתוח קורלציות בין מחיר מניה לנתוני מדיה")
        st.markdown(f"- קורלציה בין מחיר מניה לכמות הכתבות: **{corr_price_mentions:.3f}**")
        st.markdown(f"- קורלציה בין מחיר מניה לסנטימנט הממוצע: **{corr_price_sentiment:.3f}**")

        st.markdown("### 📝 מסקנות")
        st.markdown(f"- {conclusion_mentions}")
        st.markdown(f"- {conclusion_sentiment}")

        # ====== Existing visualization code ======
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("💰 Stock Closing Price")
            df_stock.index = df_stock.index.date
            st.line_chart(df_stock["Close"])
            st.markdown(f"**Overall price trend:** {trend} (from {start_price:.2f} to {end_price:.2f})")

        with col2:
            st.subheader("📰 Daily Media Mentions")

            base = alt.Chart(df_ceo).encode(
                x=alt.X("date:N", title="Date", axis=alt.Axis(labelAngle=0))
            )

            bars = base.mark_bar(color="orange").encode(
                y=alt.Y("total_mentions:Q", title="Mentions"),
                tooltip=["date", "total_mentions"]
            )

            salience_labels = base.mark_text(
                dy=-15,
                fontSize=10,
                color="black"
            ).encode(
                y="total_mentions:Q",
                text="salience_label"
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
