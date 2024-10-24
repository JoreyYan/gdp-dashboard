import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from binance_fetcher import BinanceDataFetcher  # 确保 BinanceDataFetcher 类在 binance_fetcher.py 中
import threading
import time
from streamlit_autorefresh import st_autorefresh

# 初始化数据获取器
symbol = "BTCUSDT"
interval = "1m"
start_date = "2024-06-01 00:00:00"
end_date = "2024-10-23 00:00:00"  # 初始数据获取的结束日期
data_type = 'futures'  # 或者 'spot'
fetcher = BinanceDataFetcher(symbol, interval, start_date, end_date, data_type)

# 获取初始数据
st.write("开始下载初始数据...")
fetcher.fetch_initial_data()
st.write("初始数据下载完成。")

# 定义一个后台线程，定期获取最新数据
def background_fetch():
    while True:
        fetcher.fetch_latest_data()
        time.sleep(60)  # 每分钟获取一次

fetch_thread = threading.Thread(target=background_fetch, daemon=True)
fetch_thread.start()

# Streamlit 应用布局
st.title("实时K线和MACD图，包含分段线")

# 设置自动刷新，每分钟刷新一次
st_autorefresh(interval=60 * 1000, limit=1000, key="autorefresh")

# 获取最新数据并转换为 DataFrame
df = fetcher.to_dataframe()

def create_figure(df):
    # 计算MACD指标
    ema12 = df['close'].astype(float).ewm(span=12, adjust=False).mean()
    ema26 = df['close'].astype(float).ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - signal

    df['EMA12'] = ema12
    df['EMA26'] = ema26
    df['MACD'] = macd
    df['Signal'] = signal
    df['MACD_hist'] = macd_hist

    # 创建子图
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.1, subplot_titles=('K线蜡烛图', 'MACD图'),
                        row_heights=[0.7, 0.3])

    # 添加K线蜡烛图
    fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'].astype(float),
        high=df['high'].astype(float),
        low=df['low'].astype(float),
        close=df['close'].astype(float),
        name='K线'
    ), row=1, col=1)

    # 示例：添加买卖点（根据您的策略，可以自行调整）
    buy_points_data = []
    sell_points_data = []
    for idx, row in df.iterrows():
        if idx % 15 == 0:
            buy_points_data.append({'time': row['timestamp'], 'price': float(row['close']), 'lidu': 1})
        if idx % 20 == 0:
            sell_points_data.append({'time': row['timestamp'], 'price': float(row['close']), 'lidu': 1})

    for buy_point in buy_points_data:
        fig.add_trace(go.Scatter(
            x=[buy_point['time']],
            y=[buy_point['price']],
            mode='markers+text',
            marker=dict(color='green', size=10),
            name='Buy',
            text=[f"力度: {buy_point['lidu']}"],
            textposition='top center'
        ), row=1, col=1)

    for sell_point in sell_points_data:
        fig.add_trace(go.Scatter(
            x=[sell_point['time']],
            y=[sell_point['price']],
            mode='markers+text',
            marker=dict(color='red', size=10),
            name='Sell',
            text=[f"力度: {sell_point['lidu']}"],
            textposition='bottom center'
        ), row=1, col=1)

    # 添加MACD指标
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['MACD'], mode='lines', name='MACD', line=dict(color='blue')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['Signal'], mode='lines', name='Signal', line=dict(color='orange')),
                  row=2, col=1)
    fig.add_trace(go.Bar(x=df['timestamp'], y=df['MACD_hist'], name='MACD Histogram', marker_color='green'),
                  row=2, col=1)

    # 更新布局
    fig.update_layout(
        height=800,
        xaxis_rangeslider_visible=False,
        title_text="实时K线和MACD图"
    )

    return fig

if not df.empty:
    fig = create_figure(df)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.write("暂无数据可显示。")
