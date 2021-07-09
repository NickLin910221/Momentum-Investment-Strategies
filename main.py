import datetime
import threading
import time

import loguru
import mysql.connector
import pandas as pd
import plotly.graph_objects as go
import talib
from binance.client import Client

import clear as clear
import google_sheets_insert as sheet
import LineNotify as Notify
import json

### Binance Testnet URL  
# https://testnet.binance.vision/

### SETTING 
file_name = "config.json"
with open(file_name, "r") as f:
    data = json.load(f)
    host = data["host"]
    port = data["port"]
    user = data["user"]
    password = data["password"]
    api_url = data["url"]
    api_key = data["api_key"]
    secret  = data["api_secret"]
    Line_Notify_Token = data["Line_token"]

### MANINET
client = Client(api_key,secret)

### setting ###
announce = "Monitor Bot Start Up SUCCESS."
fund = float(1)  # Testing fund
KLINE = 12      # 5min = 1, 15min = 3, 30min = 6, 1hour = 12, 4hour = 48, lday = 288
period = 300    # sec
Fee_rate = 0.001  # Fee_rate = x.xx%
porpotion = 0.1

### Logger
loguru.logger.add(f'{datetime.date.today():%Y%m%d}_profit.log' ,rotation='1 day' ,retention='7 days' ,level='ERROR')

### Check balance 
def accountbalance(asset):
    balance = client.get_asset_balance(asset=asset)
    return balance['free']
    
### Check trading_pair
def check_pair(pair):
    conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "Trading_Log",auth_plugin= "mysql_native_password")
    cursor = conn.cursor()
    sql_read_data = "SELECT * FROM Trading_Pair WHERE symbol='" + pair + "'"
    cursor.execute(sql_read_data)
    result = cursor.fetchall()

    if len(result) == 0:
        return False
    if len(result) == 1:
        return True 

def Liquid(ID,tradingtime,symbol,position,ask_orderprice):

    global fund 

    if str(position)[:4] == "LONG":
        conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "Trading_Log",auth_plugin= "mysql_native_password")
        cursor = conn.cursor()
        sql_read_data = "SELECT * FROM Trading_Pair WHERE symbol='" + symbol + "'"
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        quantity = result[0][4]
        bid_orderprice = result[0][3]
        sql_delete_data = "DELETE FROM Trading_Pair WHERE SYMBOL='" + symbol + "'"
        cursor.execute(sql_delete_data)
        fund = round(fund + round(float(quantity) * float(ask_orderprice),10) - round(float(quantity) * float(ask_orderprice) * Fee_rate,10),10)
        loguru.logger.critical({"ID" : tradingtime ,"symbol" : symbol ,"position" : position ,"bid_orderprice" : bid_orderprice ,"ask_orderprice" : ask_orderprice})
        sheet.update_sheet(ID, symbol, position, bid_orderprice, ask_orderprice, datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        sql = '''INSERT INTO Trading_History (ID,symbol,position,bid_orderprice,ask_orderprice,Time) VALUES (%s,%s,%s,%s,%s,%s)'''
        val = (ID,symbol,position,bid_orderprice,ask_orderprice,tradingtime)
        cursor.execute(sql,val)
        conn.commit()
        conn.close()

def trade(sort,tradingtime,symbol,position,orderprice):

    global fund
    quantity = 0.01

    conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "Trading_Log",auth_plugin= "mysql_native_password")
    cursor = conn.cursor()

    if sort == "BUY":
        sql_read_data = "SELECT * FROM Trading_Pair WHERE symbol='" + symbol + "'"
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        in_amount = round((quantity / float(orderprice)) - round(quantity / float(orderprice) * Fee_rate,10),10)
        if len(result) == 0:
            sql = '''INSERT INTO Trading_Pair (ID,symbol,position,orderprice,quantity) VALUES (%s,%s,%s,%s,%s)'''
            val = (tradingtime,symbol,position,orderprice,in_amount)
            cursor.execute(sql,val)
            conn.commit()
            fund = round(fund - quantity,3)
            loguru.logger.info("Take Order : LONG")
        else:
            if position in result:
                pass
            else:
                sql = '''INSERT INTO Trading_Pair (ID,symbol,position,orderprice,quantity) VALUES (%s,%s,%s,%s,%s)'''
            val = (tradingtime,symbol,position,orderprice,in_amount)
            cursor.execute(sql,val)
            conn.commit()
            fund = round(fund - quantity,3)
            loguru.logger.info("Take Order : LONG")         

    if sort == "SELL":
        sql_read_data = "SELECT * FROM Trading_Pair WHERE SYMBOL='" + symbol + "'"
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        if result[0][2] == position:
            Liquid(result[0][0],tradingtime,symbol,result[0][2],orderprice)
            loguru.logger.info("Take Order : TAKE_LONG_PROFIT")
        else:
            pass
    ###if position == "Privilege"
    conn.close()

### Trade
class trading:
    def buy(self,symbol,position,orderprice):
        if check_pair(symbol) == False:
            trade("BUY",datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),symbol,position,orderprice)
        else:
            pass
    def sell(self,symbol,position,orderprice):
        if check_pair(symbol) == True:
            trade("SELL",datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),symbol,position,orderprice)
        else:
            pass

def K_status(o,h,l,c):
    if o>c: ## red
        if o == h and c == l:
            return ## SELL>>>>BUY
        if o == h and c > l:
            return ## SELL>>>BUY
        if c == l and h > c:
            return ## SELL>>BUY
        else:
            if h - o == c - l:
                return ## SELL=BUY
            if h - o > c - l:
                return ## SELL>BUY
            if h - o < c - l:
                return ## BUY>SELL
    if o<c: ## green
        if o == l and c == h:
            return ## BUY>>>>SELL
        if o == l and h > c:
            return ## BUY>>>SELL
        if h == c and o > l:
            return ## BUY>>SELL
        else:
            if h - c == o - l:
                return ## BUY=SELL
            if h - c > o - l:
                return ## SELL>BUY
            if h - c < o - l:
                return ## BUY>SELL
def candlestick(result):
    time_index,open_index,high_index,low_index,close_index,volume_index = [],[],[],[],[],[]
    print(str(datetime.datetime.fromtimestamp(int(str(result[0][0])[:10])).strftime('%Y-%m-%d %H:%M:%S')))
    start = int(((300000*KLINE) - int(result[0][0])%(300000*KLINE)) / 300000)
    print(str(datetime.datetime.fromtimestamp(int(str(result[start][0])[:10])).strftime('%Y-%m-%d %H:%M:%S')))
    for i in range(249):
            high_index_,low_index_,volume_index_ = [],[],[]
            time_index.append(str(datetime.datetime.fromtimestamp(int(str(result[start+i*KLINE][0])[:10])).strftime('%Y-%m-%d %H:%M:%S')))
            open_index.append(result[start+i*KLINE][0][1])
            for j in range(int(KLINE)):
                high_index_.append(result[start+i*KLINE+j][2])
                low_index_.append(result[start+i*KLINE+j][3])
                volume_index_.append(float(result[start+i*KLINE+j][5]))
            high_index.append(max(high_index_))
            low_index.append(min(low_index_))
            close_index.append(result[(i*KLINE)+KLINE-1][4])
            volume_index.append(round(sum(volume_index_),2))
    data = pd.DataFrame({ "time": time_index ,
                                  "open" : open_index , 
                                  "high" : high_index , 
                                  "low" : low_index , 
                                  "close" : close_index , 
                                  "volume" : volume_index} , 
                                  columns=["time","open","high","low","close","volume"] )
    print(data)
    input()
### Loading Data
def read(symbol):
    conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "SPOT",auth_plugin= "mysql_native_password")
    cursor = conn.cursor()
    time_index,open_index,high_index,low_index,close_index,volume_index = [],[],[],[],[],[]
    start_time = str((int(time.time()/( period * KLINE )) - 200) * ( period * KLINE )) + "000"
    end_time = str(int(time.time())) + "000"

    sql_read_data = "SELECT * FROM "+ symbol +" ORDER BY Time desc LIMIT " + str(250 * KLINE) 

    try:
        cursor.execute(sql_read_data)
    except mysql.connector.errors.OperationalError as error:
        print(error)
        pass
    result = cursor.fetchall()
    result.reverse()
    conn.close()
    
    candlestick(result)

    if len(result) >= (200 * KLINE):
        for i in range(200):
            high_index_,low_index_,volume_index_ = [],[],[]
            time_index.append(str(datetime.datetime.fromtimestamp(int(str(result[i*KLINE][0])[:10])).strftime('%Y-%m-%d %H:%M:%S')))
            open_index.append(result[(i*KLINE)][1])
            for j in range(int(KLINE)):
                high_index_.append(result[(i*KLINE)+j][2])
                low_index_.append(result[(i*KLINE)+j][3])
                volume_index_.append(float(result[(i*KLINE)+j][5]))
            high_index.append(max(high_index_))
            low_index.append(min(low_index_))
            close_index.append(result[(i*KLINE)+KLINE-1][4])
            volume_index.append(round(sum(volume_index_),2))

        lenth = len(result)-(200*KLINE)
        if lenth != 0:
            high_index_,low_index_,volume_index_ = [],[],[]
            time_index.append(str(datetime.datetime.fromtimestamp(int(str(result[(200*KLINE)][0])[:10])).strftime('%Y-%m-%d %H:%M:%S')))
            open_index.append(result[(200*KLINE)][1])
            for j in range(lenth):
                    high_index_.append(result[(200*KLINE)+j][2])
                    low_index_.append(result[(200*KLINE)+j][3])
                    volume_index_.append(float(result[(200*KLINE)+j][5]))
            high_index.append(max(high_index_))
            low_index.append(min(low_index_))
            close_index.append(result[-1][4])
            volume_index.append(round(sum(volume_index_),2))
        try:
            data = pd.DataFrame({ "time": time_index ,
                                  "open" : open_index , 
                                  "high" : high_index , 
                                  "low" : low_index , 
                                  "close" : close_index , 
                                  "volume" : volume_index} , 
                                  columns=["time","open","high","low","close","volume"] )
                                  #, 
                                  #index=[time_index],)
        except ValueError:
            return []
        
        print(data)
        input()

        fig = go.Figure(data=[go.Candlestick(x=data['time'],
                        open=data['open'],
                        high=data['high'],
                        low=data['low'],
                        close=data['close'])])

        fig.show()
        fig.update_layout(xaxis_rangeslider_visible=False)
        """import matplotlib.pyplot as plt
        from mplfinance.original_flavor import candlestick_ohlc
        #from mplfinance import candlestick_ohlc
        import matplotlib.dates as mdates

        ticker = 'MCD'


        #Calc moving average
        data['MA10'] = data['close'].rolling(window=10).mean()
        data['MA60'] = data['close'].rolling(window=60).mean()
        data.reset_index(inplace=True)
        print(data)

        
        #Plot candlestick chart
        fig = plt.figure()
        
        ax1 = fig.add_subplot(111)
        ax2 = fig.add_subplot(111)
        ax3 = fig.add_subplot(111)
        ax1.xaxis_date()
        
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.show()
        ax2.plot(data.index, data['MA10'], label='MA_10')
        ax3.plot(data.index, data['MA60'], label='MA_60')
        plt.ylabel("Price")
        plt.title(ticker)
        ax1.grid(True)
        plt.legend(loc='best')
        plt.xticks(rotation=45)
        #plt.show()
        candlestick_ohlc(ax1, data.values, width=0.6, colorup='g', colordown='r')"""
        
        ### Index

        ### SMA
        #analysis.MA("open",7)
        #data["MA_7"] = talib.MA(data["open"],timeperiod = 7)
        data["MA_18"] = talib.MA(data["open"],timeperiod = 18)
        #data["MA_25"] = talib.MA(data["open"],timeperiod = 25)
        #data["MA_99"] = talib.MA(data["open"],timeperiod = 99)

        ### EMA
        #data["EMA_4"] = talib.EMA(data["close"],timeperiod = 4)
        data["EMA_6"] = talib.EMA(data["open"],timeperiod = 6)
        #data["EMA_24"] = talib.EMA(data["close"],timeperiod = 24)
        #data["EMA_100"] = talib.EMA(data["close"],timeperiod = 100)

        ### MACD
        data["macdfast"],data["macdslow"],data["MACD"] = talib.MACD(data["open"], fastperiod=12, slowperiod=26, signalperiod=9)
        data["macdfast_"],data["macdslow_"],data["MACD_"] = talib.MACD(data["open"], fastperiod=7, slowperiod=28, signalperiod=10)

        ### RSI
        data["RSI_6"] = talib.RSI(data["open"],timeperiod = 6)
        data["RSI_8"] = talib.RSI(data["open"],timeperiod = 8)
        data["RSI_12"] = talib.RSI(data["open"],timeperiod = 12)
        data["RSI_24"] = talib.RSI(data["open"],timeperiod = 24)
        data["RSI_6_"] = talib.RSI(data["close"],timeperiod = 6)
        #data["RSI_8_"] = talib.RSI(data["close"],timeperiod = 8)
        #data["RSI_12_"] = talib.RSI(data["close"],timeperiod = 12)
        #data["RSI_24_"] = talib.RSI(data["close"],timeperiod = 24)


        ### STOCH
        #data["K"], data["D"] = talib.STOCH(data["high"], data["low"], data["close"], fastk_period = 9, slowk_period = 3, slowk_matype = 0, slowd_period = 3, slowd_matype = 0)
        #data["J"] = 3 * data["K"] - 2 * data["D"]

        ### BOLLING BAND
        #data["upperband_3"], data["middleband_3"], data["lowerband_3"] = talib.BBANDS(data["close"], timeperiod=30, nbdevup=3, nbdevdn=3, matype=0)
        #data["upperband_3.5"], data["middleband_3.5"], data["lowerband_3.5"] = talib.BBANDS(data["close"], timeperiod=30, nbdevup=3.5, nbdevdn=3.5, matype=0)
        #data["upperband_5"], data["middleband_5"], data["lowerband_5"] = talib.BBANDS(data["close"], timeperiod=30, nbdevup=5, nbdevdn=5, matype=0)

        ### Volatility INDEX
        #data["ATR"] = talib.ATR(data["high"],data["low"],data["close"],timeperiod = 7)
        #data["NATR"] = talib.NATR(data["high"],data["low"],data["close"],timeperiod = 7)

        ### Candlestick
        #data = data.set_index(pd.DatetimeIndex(data["time"].values))
        

        ###Figure Index
        """go.Scatter: add a line at box"""
        fig = go.Figure(data=[go.Candlestick(x=data['time'],
                                            open=data['open'],
                                            high=data['high'],
                                            low=data['low'],
                                            close=data['close']),
                            go.Scatter(x=data.time, y=data.MA_18, line=dict(color='blue', width=1)),
                            go.Scatter(x=data.time, y=data.EMA_6, line=dict(color='yellow', width=1))])
        fig.add_trace(go.Scatter(x=data.time, y=data.MACD, line=dict(color='#e68cff', width=1)),
                      go.Scatter(x=data.time, y=data.macdslow, line=dict(color='#ff0000', width=1)),
                      go.Scatter(x=data.time, y=data.macdfast, line=dict(color='blue', width=1)))

        
        fig.update_layout(xaxis_rangeslider_visible=False)
        fig.show()
        ### Strategy
        global open_orders

        ### 策略二
        if data["close"][-1] > max(data["high"][-24:]):
            trading().buy(symbol, "above_last_high", data["close"][-1])
        if data["close"][-1] < min(data["low"][-4:]):
            trading().sell(symbol, "above_last_high", data["close"][-1])

        ### MACD快線轉正 成交量大於24根1Hr K棒均線
        if data["macdfast_"][-1] > 0 and data["macdfast_"][-2] < 0 and data["volume"][-1] > sum(data["volume"][-24:])/24 and data["macdfast_"][-1] > data["macdfast_"][-2]:
            trading().buy(symbol,"LONG_Strategy_2",data["close"][-1])
        if data["macdfast_"][-1] < 0:
            trading().sell(symbol,"LONG_Strategy_2",data["close"][-1])

        ### 策略三
        if data["EMA_6"][-2] < data["MA_18"][-2] and data["EMA_6"][-1] > data["MA_18"][-1] and data["MACD"][-1] > 0:
            trading().buy(symbol,"LONG_Strategy_3",data["close"][-1])
        #if data["macdfast"][-2] > data["macdslow"][-2] and data["macdfast"][-1] < data["macdslow"][-1]:
        #    trading().sell(symbol,"LONG_Strategy_3",data["close"][-1])
        if data["EMA_6"][-2] > data["MA_18"][-2] and data["EMA_6"][-1] < data["MA_18"][-1]:
            trading().sell(symbol,"LONG_Strategy_3",data["close"][-1])

        ### RSI 過低
        if data["RSI_6_"][-2] < 10 and data["RSI_6_"][-1] > 10:
            trading().buy(symbol,"RSIoversell",data["close"][-1])
        if data["RSI_6_"][-2] > data["RSI_6_"][-1]:
            trading().sell(symbol,"RSIoversell",data["close"][-1])

        ### Trading Record
        conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "Trading_Log",auth_plugin= "mysql_native_password")
        cursor = conn.cursor()
        open_orders = []
        sql_read_data = "SELECT * FROM Trading_Pair"
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        for i in range(len(result)):
            open_orders.append({"ID":result[i][0],"symbol":result[i][1],"position":result[i][2],"orderprice":result[i][3],"quantity":result[i][4]})
        conn.close()
        if len(open_orders)!=0:
            try:
                for x in range(len(open_orders)):
                    print(open_orders[x], "\n")
            except IndexError:
                pass

        ### 顯示餘額
        loguru.logger.info("Now Remain Fund:" + str(round(fund,10)) + " btc")
    else:
        print("Now:",len(result),"  Expect:", KLINE*200)
        pass

def job():
    while True:
        conn = mysql.connector.connect(host = host,port = port,user = user,password = password,database = "SYMBOL",auth_plugin= "mysql_native_password")
        cursor = conn.cursor()
        symbol = []
        sql_read_data = "SELECT * FROM SPOT where SYMBOL LIKE '%BTC' "
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        for i in range(len(result)):
            symbol.append(result[i][0])
        conn.close()
        for x in range(len(symbol)):
            read(symbol[x])

#loguru.logger.info("Now Remain Fund:" + str(round(fund,10)) + " btc")
clear.clear()
job()
