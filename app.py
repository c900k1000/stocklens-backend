import os
import certifi
import yfinance as yf
from fastapi import FastAPI, BackgroundTasks
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uvicorn
import logging

# 1. 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. SSL 修正
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

load_dotenv()
app = FastAPI()

# 3. 資料庫連線
def get_db_engine():
    try:
        url = os.getenv("DATABASE_URL")
        if not url: return None, "未設定 DATABASE_URL"
        # 關鍵：加上 pool_pre_ping 防止連線逾時
        engine = create_engine(url, pool_pre_ping=True)
        return engine, "OK"
    except Exception as e:
        return None, str(e)

# 4. 爬蟲任務 (OpenClaw 觸發的就是這個)
def run_crawler_task():
    logger.info("🔥 爬蟲任務啟動...")
    engine, status = get_db_engine()
    if not engine:
        logger.error(f"❌ DB 連線失敗: {status}")
        return

    try:
        # 測試抓取台積電
        stock = yf.Ticker("2330.TW")
        df = stock.history(period="1mo")
        if df.empty:
            logger.warning("抓不到資料")
            return

        df = df.reset_index()
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['symbol'] = '2330'
        
        df.to_sql('daily_prices', engine, if_exists='append', index=False)
        logger.info("✅ 2330 資料已寫入 Supabase")
    except Exception as e:
        logger.error(f"❌ 錯誤: {e}")

@app.get("/")
def home():
    return {"status": "Online", "msg": "Waiting for OpenClaw"}

@app.get("/trigger")
def trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_crawler_task)
    return {"message": "爬蟲已在背景執行"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # 這裡啟動 Web Server
    uvicorn.run(app, host="0.0.0.0", port=port)