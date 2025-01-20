import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import Tuple, Dict, List
import os

class BinanceDataLoader:
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 默认为期货模式
            }
        })
    
    def get_trades(self, symbol: str, since: int, limit: int = 1000, params: dict = {}) -> List[Dict]:
        """获取交易数据"""
        try:
            trades = self.exchange.fetch_trades(symbol, since=since, limit=limit, params=params)
            return trades
        except Exception as e:
            print(f"Error fetching trades: {e}")
            return []

    def get_funding_rate(self, symbol: str, since: int, limit: int = 1000) -> List[Dict]:
        """获取资金费率"""
        try:
            funding_rates = self.exchange.fetch_funding_rate_history(symbol, since=since, limit=limit)
            return funding_rates
        except Exception as e:
            print(f"Error fetching funding rates: {e}")
            return []

    def download_data(self,
                     spot_symbol: str = "BTC/USDT",
                     futures_symbol: str = "BTC/USDT:USDT",
                     start_date: str = "2024-01-01",
                     end_date: str = "2024-01-02",
                     save_dir: str = "data") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """下载并保存现货、永续合约交易数据和资金费率"""
        
        # 创建保存目录
        os.makedirs(save_dir, exist_ok=True)
        
        # 转换时间格式
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
        
        # 初始化数据列表
        spot_trades = []
        futures_trades = []
        funding_rates = []
        
        # 获取现货数据
        self.exchange.options['defaultType'] = 'spot'
        current_ts = start_ts
        while current_ts < end_ts:
            trades = self.get_trades(spot_symbol, current_ts)
            if not trades:
                break
            spot_trades.extend(trades)
            current_ts = trades[-1]['timestamp'] + 1
            time.sleep(self.exchange.rateLimit / 1000)  # 遵守速率限制
            
        # 获取永续合约数据
        self.exchange.options['defaultType'] = 'future'
        current_ts = start_ts
        while current_ts < end_ts:
            trades = self.get_trades(futures_symbol, current_ts)
            if not trades:
                break
            futures_trades.extend(trades)
            current_ts = trades[-1]['timestamp'] + 1
            time.sleep(self.exchange.rateLimit / 1000)
            
        # 获取资金费率
        current_ts = start_ts
        while current_ts < end_ts:
            rates = self.get_funding_rate(futures_symbol, current_ts)
            if not rates:
                break
            funding_rates.extend(rates)
            current_ts = rates[-1]['timestamp'] + 1
            time.sleep(self.exchange.rateLimit / 1000)
        
        # 转换为DataFrame
        spot_df = pd.DataFrame(spot_trades)
        futures_df = pd.DataFrame(futures_trades)
        funding_df = pd.DataFrame(funding_rates)
        
        # 处理数据
        for df in [spot_df, futures_df, funding_df]:
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 保存数据
        date_str = f"{start_date}_to_{end_date}"
        if not spot_df.empty:
            spot_df.to_csv(f"{save_dir}/spot_trades_{spot_symbol.replace('/', '')}_{date_str}.csv", index=False)
        if not futures_df.empty:
            futures_df.to_csv(f"{save_dir}/futures_trades_{futures_symbol.replace('/', '_')}_{date_str}.csv", index=False)
        if not funding_df.empty:
            funding_df.to_csv(f"{save_dir}/funding_rates_{futures_symbol.replace('/', '_')}_{date_str}.csv", index=False)
        
        return spot_df, futures_df, funding_df

# 使用示例
if __name__ == "__main__":
    loader = BinanceDataLoader()
    spot_df, futures_df, funding_df = loader.download_data(
        spot_symbol="BTC/USDT",
        futures_symbol="BTC/USDT:USDT",
        start_date="2024-01-01",
        end_date="2024-01-02",
        save_dir="data"
    )
    
    print("Spot trades shape:", spot_df.shape if not spot_df.empty else "No data")
    print("Futures trades shape:", futures_df.shape if not futures_df.empty else "No data")
    print("Funding rates shape:", funding_df.shape if not funding_df.empty else "No data")