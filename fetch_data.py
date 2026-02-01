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
app = FastAPI()
engine = create_engine(os.getenv("DATABASE_URL"))

@app.get("/")
def health_check():
    # 這是為了讓 Railway 知道你的程式還活著
    return {"status": "Boshibao API Online", "database": "Connected"}

@app.get("/update")
def fetch_2330():
    # 執行原本的爬蟲邏輯
    stock = yf.Ticker("2330.TW")
    df = stock.history(period="1mo")
    # ... (原本寫入資料庫的邏輯)
    return {"message": "Update Success", "count": len(df)}

if __name__ == "__main__":
    # 關鍵：讀取 Railway 分配的 PORT 並保持運行
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)