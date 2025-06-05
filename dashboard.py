import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import yfinance as yf
from datetime import timedelta

# פרטי פרויקט
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

st.title("📊 CEO & Stock Dashboard")

# בחירת מנכ"ל
ceo_name = st.selectbox("Choose a CEO:", list(ceo_to_company.keys()))
company_name, ticker = ceo_to_company[ceo_name]

client = bigquery.Client(project=project_id)

# שליפת תאריכים מהטבלה
query = f"""
SELECT MIN(date) AS start_date, MAX(date) AS end_date
FROM `{project_id}.{dataset}.{table}`
WHERE name = "{ceo_name}"
"""
df_dates = client.query(query).result().to_dataframe()
start_date = df_dates["start_date"][0]
end_date = pd.to_datetime(df_dates["end_date"][0]) + timedelta(days=1)

# מחירי מניה
df_stock = yf.download(ticker, start=start_date, end=end_date).reset_index()[["Date", "Close"]]

# נתוני סנטימנט
summary_query = f"""
SELECT DATE, SUM(numMentions) AS numMentions,
       ROUND(AVG(avgSalience), 3) AS avgSalience,
       ROUND(AVG(sentiment_score), 3) AS sentiment_score
FROM `{project_id}.{dataset}.{table}`
WHERE name = "{ceo_name}"
  AND DATE BETWEEN "{start_date.strftime('%Y-%m-%d')}" AND "{(end_date - timedelta(days=1)).strftime('%Y-%m-%d')}"
GROUP BY DATE ORDER BY DATE
"""
df_summary = client.query(summary_query).result().to_dataframe()
df_summary["Date"] = pd.to_datetime(df_summary["DATE"])
df_merged = pd.merge(df_stock, df_summary, on="Date", how="left")

# גרף
fig = go.Figure()
fig.add_trace(go.Scatter(x=df_merged["Date"], y=df_merged["Close"], name="Stock Price", mode="lines+markers"))
fig.add_trace(go.Bar(x=df_merged["Date"], y=df_merged["sentiment_score"], name="Sentiment", yaxis="y2", opacity=0.6))

fig.update_layout(
    title=f"{ceo_name} ({company_name}) – Sentiment vs Stock",
    xaxis=dict(title="Date"),
    yaxis=dict(title="Price"),
    yaxis2=dict(title="Sentiment", overlaying="y", side="right")
)

st.plotly_chart(fig)
