import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import numpy as np

BOT_TOKEN = os.getenv("BOT_TOKEN")

chat_ids = [
    #"1070509960",
    #"1937479700",
    "5034473353",
]

def send_message(text):
    for chat_id in chat_ids:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        requests.post(url, data=payload)

def wait_until_next_run():
    now = datetime.now()

    # next minute target (00 or 30)
    if now.minute < 30:
        target = now.replace(minute=30, second=5, microsecond=0)
    else:
        target = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)

    sleep_seconds = (target - now).total_seconds()
    time.sleep(max(0, sleep_seconds))

last_signal = None   # store previous signal
send_message("🚀 Delta ETH Bot Started")
while True:

    print("Running at:", datetime.now())

    # ======================================
    # FETCH DATA FROM DELTA EXCHANGE
    # ======================================
    
    end = int(time.time()) - 60
    start = end - 10 * 24 * 3600  # 200 candles of 30m
    
    url = "https://api.delta.exchange/v2/history/candles"
    
    params = {
        "symbol": "ETHUSD",        # ETH perpetual on Delta India
        "resolution": "30m",
        "start": start,
        "end": end
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    print("API response:", data)
    
    if not data.get("success"):
        raise Exception(f"Delta API error: {data}")
    
    candles = data["result"]
    
    if not candles:
        raise Exception("No candle data returned")
    
    # Create dataframe
    df = pd.DataFrame(candles)
    
    # Rename columns to match your strategy
    df = df.rename(columns={
        "time": "Open_time",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })
    
    # Convert timestamp (Delta uses seconds)
    df["Open_time"] = pd.to_datetime(df["Open_time"], unit="s")
    
    # Convert numeric columns
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = df[col].astype(float)
    
    # Delta doesn't provide this column, but your code expects it
    df["Number_of_trades"] = 0

    # ==========================
    # RSI FUNCTION
    # ==========================
    def calculate_rsi(series, length=14):
        delta = series.diff()
    
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
    
        gain = pd.Series(gain, index=series.index)
        loss = pd.Series(loss, index=series.index)
    
        avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    
        return rsi
    
    
    df = df.loc[:,['Open_time','Open','Close','High', 'Low', 'Volume','Number_of_trades']]
    
    # convert to datetime
    df['Open_time'] = pd.to_datetime(df['Open_time'])
    
    # convert UTC → IST
    df['Open_time'] = (df['Open_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None))
    
    df2 =df.copy()
    
    # =====================================================
    # INDICATORS
    # =====================================================
    # =====================================================
    # ✅ CCI (60)
    # =====================================================
    
    # Typical Price
    df2["hlc3"] = (df2["High"] + df2["Low"] + df2["Close"]) / 3
    df2["ma"] = df2["hlc3"].rolling(window=60).mean()
    
    # Mean Deviation
    df2["mean_dev"] = df2["hlc3"].rolling(window=60).apply(lambda x: np.mean(np.abs(x - np.mean(x))),raw=True)
    
    # CCI Formula
    df2["CCI_60"] = (df2["hlc3"] - df2["ma"]) / (0.015 * df2["mean_dev"])
    
   # ✅ CCI EMA 7 (Smoothing)
    df2["CCI_EMA"] = df2["CCI_60"].ewm(span=7, adjust=False).mean()
    df2['OUTPUT'] = np.where(df2['CCI_60'] > df2.CCI_EMA, 'Pass', 'Fail')
    df2['Decision'] = np.where(df2['CCI_60'] > df2.CCI_EMA, 'Long', 'Short')
    
    # ✅ EMA 7 and EMA 200
    df2["EMA7"] = df2["Close"].ewm(span=7, adjust=False).mean()
    df2["EMA200"] = df2["Close"].ewm(span=200, adjust=False).mean()
    
    df2['EMA7_CROSS'] = np.where(df2["Close"] > df2['EMA7'], 'Pass', 'Fail')
    df2['EMA200_CROSS'] = np.where(df2["Close"] > df2['EMA200'], 'Pass', 'Fail')
    
    # Pass if either EMA condition passes
    df2["EMA_PASS"] = np.where(
        (df2['EMA7_CROSS'] == "Pass") | (df2['EMA200_CROSS'] == "Pass"),
        "Pass",
        "Fail"
    )
    df2.drop(['hlc3','ma','mean_dev','High','Low','Volume','Number_of_trades',
              'EMA7_CROSS','EMA200_CROSS'], axis=1, inplace=True)
    
    # RSI
    df2['RSI'] = calculate_rsi(df2["Close"])
    df2['RSI_OUTPUT'] = np.where(df2['RSI'] > 40, 'Pass', 'Fail')
    
    # Difference
    df2['Diff_CCI'] = df2.CCI_60 - df2.CCI_EMA
    
    # Signal
    df2['Signal'] = np.where(
        (df2['OUTPUT'] == 'Pass') & (abs(df2['Diff_CCI']) > 4) & (df2['EMA_PASS'] == 'Pass'),
        'Long Entry',
        np.where(
            (df2['OUTPUT'] == 'Fail') & (abs(df2['Diff_CCI']) > 4) & (df2['EMA_PASS'] == 'Fail'),
            'Short Entry',
            'No Trade'
        )
    )

    filtered_df = df2[["Open_time", "Signal", "Close"]]


    latest = filtered_df.iloc[-2]
    
    open_time = pd.to_datetime(latest["Open_time"]).strftime("%Y-%m-%d %H:%M")
    close = latest["Close"]
    signal = latest["Signal"]

  # check if signal changed
    if signal != last_signal:
    
        msg = f"""
    🚨 Trading Signal
    Time: {open_time}
    Closing Price: {close}
    Signal: {signal}
    """
    
        send_message(msg)
    
        data = {
            "Running at": datetime.now(),
            "Open_time": [open_time],
            "Closing Price": [close],
            "Signal": [signal]
        }
    
        df_save = pd.DataFrame(data)
    
        file = "signals.xlsx"
    
        if not os.path.exists(file):
            df_save.to_excel(file, index=False)
        else:
            with pd.ExcelWriter(file, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                df_save.to_excel(
                    writer,
                    index=False,
                    header=False,
                    startrow=writer.sheets["Sheet1"].max_row
                )
    
        last_signal = signal
    
    wait_until_next_run()
