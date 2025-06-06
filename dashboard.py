import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime
from google.cloud import bigquery

# === פרטים קבועים לדשבורד הניסיוני ===
ceo_name = "Jensen Huang"
company_name, ticker = "NVIDIA", "NVDA"
start_date = datetime(2025, 4, 1)
end_date = datetime(2025, 4, 3)
project_id = "bigdata456"
dataset = "Big_Data_456_data"
table = "ceo_articles_nvidia_test"

st.set_page_config(page_title="Dashboard - ניסוי", layout="wide")
st.title("📊 Media & Stock Dashboard (ניסוי על Jensen Huang)")

st.markdown(f"**מנכ\"ל:** {ceo_name}  |  **חברה:** {company_name} ({ticker})")
st.markdown(f"**טווח תאריכים:** {start_date.date()} עד {end_date.date()}")

# === פונקציה לשליפת נתוני GDELT היומיים ===
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
    
    result = client.query(query, job_config=job_config).result().to_dataframe()

    def classify_sentiment(score):
        if pd.isna(score):
            return "⏺ נייטרלי"
        elif score > 0.2:
            return "😊 חיובי"
        elif score < -0.2:
            return "☹ שלילי"
        else:
            return "⏺ נייטרלי"
    
    result["sentiment_category"] = result["avg_sentiment"].apply(classify_sentiment)
    return result

# כפתור להרצת הניתוח
if st.button("🔍 הפעל ניתוח"):
    
    # === שליפת נתוני מניה ===
    df_stock = yf.download(ticker, start=start_date, end=end_date + pd.Timedelta(days=1))
    
    if df_stock.empty:
        st.warning("⚠️ לא נמצאו נתוני מניה בטווח שנבחר")
    else:
        start_price = df_stock["Close"].iloc[0].item()
        end_price = df_stock["Close"].iloc[-1].item()
        trend = "📈 עלייה" if end_price > start_price else "📉 ירידה" if end_price < start_price else "➖ ללא שינוי"

        st.subheader("💰 גרף מניה - מחיר סגירה")
        st.line_chart(df_stock["Close"])
        st.markdown(f"**תנועת מחיר כוללת:** {trend} (מ־{start_price:.2f} ל־{end_price:.2f})")

    # === שליפת נתוני GDELT ===
    df_ceo_stats = get_ceo_daily_stats(
        project_id=project_id,
        dataset=dataset,
        table=table,
        ceo_name=ceo_name,
        start_date=start_date.date(),
        end_date=end_date.date()
    )

    if df_ceo_stats.empty:
        st.warning("⚠️ לא נמצאו נתונים תקשורתיים בטווח שנבחר")
    else:
        st.subheader("📰 ניתוח תקשורתי יומי (GDELT)")
        
        st.dataframe(df_ceo_stats.style.format({
            "avg_sentiment": "{:.2f}",
            "avg_salience": "{:.2f}"
        }))
        
        st.markdown("**🎯 ממוצע סנטימנט יומי:**")
        st.bar_chart(df_ceo_stats.set_index("date")["avg_sentiment"])

        st.markdown("**📢 כמות האזכורים:**")
        st.bar_chart(df_ceo_stats.set_index("date")["total_mentions"])

        st.markdown("**🔥 ממוצע Salience:**")
        st.line_chart(df_ceo_stats.set_index("date")["avg_salience"])
