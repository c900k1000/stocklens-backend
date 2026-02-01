import os
import certifi
import yfinance as yf
from fastapi import FastAPI
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uvicorn

# 修正憑證路徑
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

load_dotenv()

# 初始化 FastAPI
app = FastAPI()

# 建立資料庫連線 (請確保 Variables 裡的 URL 包含 .pulzbznngdwumyiafelv)
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/")
def home():
    # 這是給 Railway 檢查用的，網址點開會看到這個
    return {"status": "Boshibao FinTech API is Online", "msg": "Hello StockLens"}

@app.get("/update")
def run_scraper():
    try:
        stock = yf.Ticker("2330.TW")
        df = stock.history(period="1mo")
        if df.empty:
            return {"status": "error", "message": "抓不到資料"}
        
        # 整理與寫入資料庫
        df = df.reset_index()
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['symbol'] = '2330'
        df.to_sql('daily_prices', engine, if_exists='append', index=False)
        
        return {"status": "success", "message": "2330.TW 資料已更新"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # 關鍵：讓程式監聽 Railway 分配的 PORT 且不要跑完就結束
    port = int(os.environ.get("PORT", 8000))
    # host 必須設定為 0.0.0.0 才能讓外部連線
    uvicorn.run(app, host="0.0.0.0", port=port)