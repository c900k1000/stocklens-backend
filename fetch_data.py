import os
import certifi
import yfinance as yf
import pandas as pd
from fastapi import FastAPI, BackgroundTasks
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import uvicorn
import logging

# 設定日誌，方便除錯
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 修正 SSL
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

load_dotenv()
app = FastAPI()

# 測試資料庫連線
def get_db_engine():
    try:
        url = os.getenv("DATABASE_URL")
        if not url:
            return None, "未設定 DATABASE_URL"
        # 確保使用 pool_pre_ping 防止連線中斷
        engine = create_engine(url, pool_pre_ping=True)
        return engine, "OK"
    except Exception as e:
        return None, str(e)

# 實際執行的爬蟲任務 (背景執行，不會卡住網頁)
def run_crawler_task():
    logger.info("🔥 OpenClaw 觸發爬蟲任務開始...")
    engine, status = get_db_engine()
    
    if not engine:
        logger.error(f"❌ 資料庫連線失敗: {status}")
        return

    try:
        # 這裡放你的 50 檔股票邏輯，先用台積電測試
        target_stocks = ["2330.TW"] 
        
        for symbol in target_stocks:
            logger.info(f"正在抓取 {symbol}...")
            stock = yf.Ticker(symbol)
            df = stock.history(period="1mo")
            
            if df.empty:
                logger.warning(f"{symbol} 抓不到資料")
                continue

            # 資料整理
            df = df.reset_index()
            df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df['symbol'] = symbol.split('.')[0] # 轉成 2330
            
            # 寫入資料庫
            df.to_sql('daily_prices', engine, if_exists='append', index=False)
            logger.info(f"✅ {symbol} 資料已寫入 Supabase")
            
    except Exception as e:
        logger.error(f"❌ 爬蟲發生錯誤: {e}")

@app.get("/")
def home():
    """ 這是給 Railway 檢查心跳用的，確保網址能打開 """
    return {"status": "StockLens Backend Online", "waiting_for": "OpenClaw"}

@app.get("/trigger")
def trigger_by_openclaw(background_tasks: BackgroundTasks):
    """ 這就是 OpenClaw 要呼叫的按鈕 """
    background_tasks.add_task(run_crawler_task)
    return {"message": "收到指令，爬蟲正在背景執行中...", "target": "Supabase"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)