# -*- coding: utf-8 -*-
"""
Created on Sat Jan 18 20:17:31 2025

@author: 1
"""

import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time

class BinanceDataLoader:
    def __init__(self):
        # 初始化币安交易所接口
        self.spot_exchange = ccxt.binance({
            'proxies': {"http": "socks5://localhost:10808",
                        "https": "socks5://localhost:10808"},
            'timeout': 10000,
            'enableRateLimit': True,
        })
        
        self.futures_exchange = ccxt.binanceusdm({
            'proxies': {"http": "socks5://localhost:10808",
                        "https": "socks5://localhost:10808"},
            'timeout': 10000,
            'enableRateLimit': True,
        })

    def fetch_ohlcv(self, symbol, timeframe='1m', start_time=None, end_time=None, is_futures=False):
        """
        获取OHLCV数据
        
        参数:
            symbol: 交易对符号
            timeframe: 时间周期 (默认: '1m')
            start_time: 开始时间
            end_time: 结束时间
            is_futures: 是否为永续合约
        """
        exchange = self.futures_exchange if is_futures else self.spot_exchange
        
        # 转换时间戳
        start_timestamp = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)
        
        all_data = []
        current_timestamp = start_timestamp
        
        while current_timestamp < end_timestamp:
            try:
                # 获取K线数据
                ohlcv = exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    since=current_timestamp,
                    limit=1000  # binance的限制
                )
                
                if not ohlcv:
                    break
                
                all_data.extend(ohlcv)
                
                # 更新时间戳
                current_timestamp = ohlcv[-1][0] + 1
                
                # 避免触发频率限制
                time.sleep(exchange.rateLimit / 1000)
                
            except Exception as e:
                print(f"发生错误: {e}")
                time.sleep(10)
                continue
                
        # 转换为DataFrame
        df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        return df

def main():
    # 创建加载器实例
    loader = BinanceDataLoader()
    
    # 设置时间范围
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)  # 获取最近1天的数据
    
    # 下载现货数据
    spot_data = loader.fetch_ohlcv(
        symbol='BTC/USDT',
        timeframe='1m',
        start_time=start_time,
        end_time=end_time,
        is_futures=False
    )
    
    # 下载永续合约数据
    futures_data = loader.fetch_ohlcv(
        symbol='BTC/USDT',
        timeframe='1m',
        start_time=start_time,
        end_time=end_time,
        is_futures=True
    )
    
    # 保存数据
    spot_data.to_csv('btc_usdt_spot_1m.csv')
    futures_data.to_csv('btc_usdt_futures_1m.csv')
    
    print("数据下载完成！")

if __name__ == "__main__":
    main()