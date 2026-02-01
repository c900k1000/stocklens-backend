import os
import certifi
import yfinance as yf
from fastapi import FastAPI
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uvicorn

# 修正 SSL 憑證問題 (針對 Windows 及部分 Linux 環境)
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

load_dotenv()

app = FastAPI()

# 建立資料庫連線
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/")
def home():
    return {"status": "Boshibao FinTech API is Online", "project": "StockLens"}

@app.get("/update_2330")
def update_stock():
    try:
        print("正在抓取 2330.TW 資料...")
        stock = yf.Ticker("2330.TW")
        df = stock.history(period="1mo")
        
        if df.empty:
            return {"error": "抓不到資料"}

        # 整理資料格式
        df = df.reset_index()
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['symbol'] = '2330'
        
        # 寫入 Supabase
        df.to_sql('daily_prices', engine, if_exists='append', index=False)
        return {"message": "台積電資料更新成功", "rows": len(df)}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    # Railway 會自動分配 PORT，若無則預設 8000
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)