import requests
import pandas as pd
from datetime import datetime, timedelta
import threading

class BinanceDataFetcher:
    def __init__(self, symbol, interval, start_date, end_date, data_type='spot'):
        self.symbol = symbol
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.data_type = data_type
        self.base_url = self._get_base_url()
        self.data = []
        self.lock = threading.Lock()  # 用于线程安全

    def _get_base_url(self):
        if self.data_type == 'spot':
            return 'https://api.binance.com/api/v3/klines'
        elif self.data_type == 'futures':
            return 'https://fapi.binance.com/fapi/v1/klines'
        else:
            raise ValueError("data_type must be either 'spot' or 'futures'")

    def fetch_initial_data(self):
        """
        获取初始的历史数据
        """
        symbol = self.symbol
        interval = self.interval
        start_date = self.start_date
        end_date = self.end_date

        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        all_klines = []

        while start_ts < end_ts:
            print('下载数据 (UTC):', pd.to_datetime(start_ts, unit='ms'))
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': start_ts,
                'endTime': end_ts,
                'limit': 1000
            }

            response = requests.get(self.base_url, params=params)
            data = response.json()

            if not data:
                break

            all_klines.extend(data)

            # 更新 start_ts 为最后一个 K 线的结束时间
            start_ts = data[-1][6] + 1  # 使用 close_time + 1 ms

            # 检查是否达到数据末尾
            if len(data) < 1000:
                break

        with self.lock:
            self.data = all_klines

    def fetch_latest_data(self):
        """
        获取最新的一条 K 线数据
        """
        symbol = self.symbol
        interval = self.interval

        with self.lock:
            if not self.data:
                last_close_time = int(datetime.utcnow().timestamp() * 1000)
            else:
                last_close_time = self.data[-1][6]  # 使用 close_time

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': 1
        }

        response = requests.get(self.base_url, params=params)
        data = response.json()

        if data:
            with self.lock:
                # 避免重复数据
                if data[0][0] > self.data[-1][0]:
                    self.data.append(data[0])
                elif data[0][0] == self.data[-1][0]:
                    self.data[-1] = data[0]

    def to_dataframe(self):
        with self.lock:
            df = pd.DataFrame(self.data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_av', 'trades', 'tb_base_av',
                'tb_quote_av', 'ignore'
            ]).copy()

        # 转换时间戳格式
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

        # 移除重复数据
        df = df.drop_duplicates(subset=['timestamp'])

        # 按时间排序
        df = df.sort_values(by='timestamp')

        return df

    def save_to_csv(self, filename):
        df = self.to_dataframe()
        df.to_csv(filename, index=False)
