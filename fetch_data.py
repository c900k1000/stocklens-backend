import os
import certifi
import yfinance as yf
import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uvicorn

# 1. 修正 SSL 憑證路徑
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

load_dotenv()
app = FastAPI()

# 2. 建立資料庫連線
# 注意：這會從 Railway 的環境變數讀取
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/")
def home():
    return {"status": "Boshibao API Online", "project": "StockLens"}

@app.get("/update")
def run_scraper():
    """點擊網址/update 會執行這段"""
    try:
        print("正在抓取 2330.TW 的歷史資料...")
        stock = yf.Ticker("2330.TW")
        df = stock.history(period="1mo")
        
        if df.empty:
            return {"status": "error", "message": "抓不到資料"}

        # 整理格式
        df = df.reset_index()
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['symbol'] = '2330'
        
        # 寫入 Supabase
        df.to_sql('daily_prices', engine, if_exists='append', index=False)
        return {"status": "success", "message": "2330.TW 資料更新成功", "rows": len(df)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # 3. 關鍵：讓程式在雲端持續運行並監聽 Port
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)