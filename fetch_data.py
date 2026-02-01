import os
import certifi

# 修正 Windows 下 SSL 憑證找不到的問題 (Error 77)
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 載入環境變數
load_dotenv()

# 取得資料庫連線字串
DATABASE_URL = os.getenv("DATABASE_URL")


def fetch_stock_data(symbol: str = "2330.TW", period: str = "1mo") -> pd.DataFrame:
    """
    使用 yfinance 抓取台股歷史日線資料
    
    Args:
        symbol: 股票代碼 (例如: 2330.TW 為台積電)
        period: 資料期間 (1mo = 一個月)
    
    Returns:
        DataFrame 包含歷史價格資料
    """
    print(f"正在抓取 {symbol} 的歷史資料...")
    
    # 使用 yfinance 下載資料
    stock = yf.Ticker(symbol)
    df = stock.history(period=period)
    
    if df.empty:
        print(f"警告: 無法取得 {symbol} 的資料")
        return pd.DataFrame()
    
    # 重設索引，將日期變成欄位
    df = df.reset_index()
    
    # 重新命名欄位以對應 Supabase daily_prices 表
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    
    # 新增 symbol 欄位
    df["symbol"] = symbol.replace(".TW", "")  # 移除 .TW 後綴，只保留股票代號
    
    # 只保留需要的欄位
    df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]
    
    # 確保日期格式正確 (移除時區資訊)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # 將數值欄位四捨五入到小數點後兩位
    numeric_columns = ["open", "high", "low", "close"]
    df[numeric_columns] = df[numeric_columns].round(2)
    
    # 確保 volume 為整數
    df["volume"] = df["volume"].astype(int)
    
    print(f"成功抓取 {len(df)} 筆資料")
    return df


def save_to_database(df: pd.DataFrame, table_name: str = "daily_prices") -> None:
    """
    將資料寫入 Supabase PostgreSQL 資料庫
    
    Args:
        df: 要寫入的 DataFrame
        table_name: 目標資料表名稱
    """
    if df.empty:
        print("沒有資料可寫入")
        return
    
    if not DATABASE_URL:
        print("錯誤: 找不到 DATABASE_URL 環境變數")
        return
    
    print(f"正在連接資料庫...")
    
    try:
        # 建立資料庫連線
        engine = create_engine(DATABASE_URL)
        
        # 寫入資料到資料庫
        # if_exists="append" 表示新增資料，不會覆蓋現有資料
        # if_exists="replace" 表示會先刪除資料表再重新建立
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",  # 使用 append 避免刪除現有資料
            index=False
        )
        
        print(f"成功將 {len(df)} 筆資料寫入 {table_name} 資料表")
        
    except Exception as e:
        print(f"寫入資料庫時發生錯誤: {e}")
    finally:
        engine.dispose()


def main():
    """主程式"""
    print("=" * 50)
    print("台股爬蟲程式啟動")
    print("=" * 50)
    
    # 抓取台積電 (2330.TW) 一個月的歷史資料
    df = fetch_stock_data(symbol="2330.TW", period="1mo")
    
    if not df.empty:
        # 顯示資料預覽
        print("\n資料預覽:")
        print(df.head())
        print(f"\n資料筆數: {len(df)}")
        
        # 寫入資料庫
        save_to_database(df)
    
    print("\n" + "=" * 50)
    print("程式執行完成")
    print("=" * 50)


if __name__ == "__main__":
    main()
