import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3

# Function to initialize the database
def init_db():
    conn = sqlite3.connect('crypto_data.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS crypto_data (
            timestamp TEXT,
            token TEXT,
            price REAL,
            PRIMARY KEY (timestamp, token)
        )
    ''')
    conn.commit()
    conn.close()

# Function to get Unix timestamp from a date string
def get_unix_timestamp(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())

# Function to round datetime to the nearest hour
def round_to_nearest_hour(dt):
    if dt.minute >= 30:
        return dt.replace(second=0, microsecond=0, minute=0) + timedelta(hours=1)
    else:
        return dt.replace(second=0, microsecond=0, minute=0)

# Function to fetch candle data from the database or API
def fetch_candle_data(token, start_date, end_date):
    conn = sqlite3.connect('crypto_data.db')
    c = conn.cursor()
    records_added = 0

    c.execute("""
        SELECT timestamp, price FROM crypto_data
        WHERE token = ? AND timestamp >= ? AND timestamp <= ?
    """, (token, start_date + " 00:00:00", end_date + " 23:59:59"))
    data = c.fetchall()

    if not data:
        url = f"https://api.coingecko.com/api/v3/coins/{token}/market_chart/range"
        params = {
            'vs_currency': 'usd',
            'from': get_unix_timestamp(start_date),
            'to': get_unix_timestamp(end_date)
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            conn.close()
            raise Exception(f"Error fetching data for {token}: {response.text}")

        api_data = response.json()['prices']
        formatted_data = [(datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S'), token, price) for ts, price in api_data]
        c.executemany("""
            INSERT OR IGNORE INTO crypto_data (timestamp, token, price)
            VALUES (?, ?, ?)
        """, formatted_data)
        conn.commit()
        records_added = len(api_data)
        data = formatted_data
    else:
        data = [(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S'), token, price) for ts, price in data]

    conn.close()
    df = pd.DataFrame(data, columns=['timestamp', 'token', f'price_{token}'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].apply(round_to_nearest_hour)
    df.drop('token', axis=1, inplace=True)
    return df, records_added

# Function to generate and display the pair chart
def get_pair_chart(start_date, end_date, df1, df2):
    try:
        merged_data = pd.merge(df1, df2, on='timestamp', how='inner')
        merged_data['ratio'] = merged_data[merged_data.columns[1]] / merged_data[merged_data.columns[2]]
        merged_data['timestamp'] = pd.to_datetime(merged_data['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

        st.write('Last 5 Records:')
        st.write(merged_data.tail())

        initial_ratio = merged_data['ratio'].iloc[0]
        final_ratio = merged_data['ratio'].iloc[-1]
        percentage_change = ((final_ratio - initial_ratio) / initial_ratio) * 100
        direction = "up" if percentage_change > 0 else "down"

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=merged_data['timestamp'], y=merged_data['ratio'], mode='lines', name='Ratio'))
        fig.update_layout(
            title=f'{token1.title()} is {direction} {abs(percentage_change):.2f}% on {token2.title()} from {start_date} to {end_date}',
            xaxis_title='Date and Time',
            yaxis_title='Price Ratio',
            xaxis=dict(type='date', tickformat='%Y-%m-%d %H:%M:%S', rangeslider_visible=True),
            font=dict(size=11)
        )
        st.plotly_chart(fig)

    except Exception as e:
        st.error(f"An error occurred: {e}")

# Streamlit app setup
init_db()
st.title("Cryptocurrency Price Ratio Tracker")

today = datetime.today()
coins = ['bitcoin', 'solana', 'ethereum', 'cardano', 'fantom', 'near', 'pyth-network']
start_date = st.date_input("Start Date", value=pd.to_datetime("01-01-2023"))
end_date = st.date_input("End Date", value=pd.to_datetime(today))
token1 = st.selectbox("Select the first token:", coins)
token2 = st.selectbox("Select the second token:", coins)

if st.button('Analyze Price Ratio'):
    df1, records_added1 = fetch_candle_data(token1, str(start_date), str(end_date))
    df2, records_added2 = fetch_candle_data(token2, str(start_date), str(end_date))
    if records_added1 + records_added2 > 0:
        st.success(f"Records have successfully been added to the database")

    get_pair_chart(str(start_date), str(end_date), df1, df2)
