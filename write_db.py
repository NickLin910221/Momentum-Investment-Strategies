import json
import threading

import loguru
import mysql.connector
import websocket
from binance.client import Client

import LineNotify as Notify

websocket.enableTrace(True)

### SETTING
file_name = "config.json"
with open(file_name, "r") as f:
    data = json.load(f)
    host = data["host"]
    port = data["port"]
    user = data["user"]
    password = data["password"]
    Line_Notify_Token = data["Line_token"]

### Write Database
def write_database(symbol, Time, open_price, high_price, low_price, close_price, volume):
    conn = mysql.connector.connect(host = host, port = port, user = user, password = password, database = "SPOT", auth_plugin = "mysql_native_password")
    cursor = conn.cursor()
    search = '''select * from ''' + symbol + ''' where Time="''' + str(Time) + '''"'''
    cursor.execute(search)
    result = cursor.fetchall()
    if len(result) == 0:
        try:
            sql = '''INSERT INTO ''' + symbol + ''' (Time,Open,High,Low,Close,Volume) VALUES (%s,%s,%s,%s,%s,%s)'''
            val = (Time,open_price,high_price,low_price,close_price,volume)
            cursor.execute(sql,val)
            conn.commit()
            loguru.logger.success("Write Database : success")
    
        except json.decoder.JSONDecodeError:
            loguru.logger.error( symbol + "\a Write Database Error！ ")
            pass
    if len(result) > 0:
        try:
            sql = '''UPDATE ''' + symbol + ''' set Time="''' + str(Time) + '''",Open="''' + str(open_price) + '''",High="''' + str(high_price) + '''",Low="''' + str(low_price) + '''",Close="''' + str(close_price) + '''",Volume="''' + str(volume) + '''" where Time="''' + str(Time) + '''"'''
            cursor.execute(sql)
            conn.commit()
            loguru.logger.success("UPDATE Database : success")
        
        except json.decoder.JSONDecodeError:
            loguru.logger.error( symbol + "\a Write Database Error！ ")
            pass
    print(symbol,Time,open_price,high_price,low_price,close_price,volume)

def on_open(ws):
    loguru.logger.trace(" Connection is opened！ ") 

def on_message(ws, message):
    def run(*args):
        jsLoads = json.loads(message)
        interval    = jsLoads['data']['k']['i']
        symbol      = jsLoads['data']['k']['s']
        Time        = jsLoads['data']['k']['t']
        open_price  = jsLoads['data']['k']['o']
        high_price  = jsLoads['data']['k']['h']
        low_price   = jsLoads['data']['k']['l']
        close_price = jsLoads['data']['k']['c']
        volume      = jsLoads['data']['k']['v']
        loguru.logger.info(" Pair " + symbol + " Get new KLINES Tick ！ ")   
        write_database(symbol, Time, open_price, high_price, low_price, close_price, volume)

    threading.Thread(target=run).start()

def on_error(ws, error):
    Notify.SendMessageToLineNotify(error,Line_Notify_Token)

def on_close(ws):
    loguru.logger.warning(" Connection is closed！ ") 
    Notify.SendMessageToLineNotify(" Write database program Connection is closed！ ",Line_Notify_Token)

while True:
    try:
        ### Streams
        streams = ""
        conn = mysql.connector.connect(host = host, port = port, user = user, password = password, database = "symbol", auth_plugin = "mysql_native_password" )
        cursor = conn.cursor()
        sql_read_data = "SELECT * FROM SPOT"
        cursor.execute(sql_read_data)
        result = cursor.fetchall()
        conn.close()
        for i in range(len(result)):
            a = result[i][0]
            if i != int(len(result)-1):
                streams = streams + str(a.lower()) + "@kline_5m/"
            if i == int(len(result)-1):
                streams = streams + str(a.lower()) + "@kline_5m"
        ### Binance  Websocket API
        socket = "wss://stream.binance.com/stream?streams=" + streams
        print(socket)
        ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_error = on_error, on_close=on_close)
        ws.run_forever()
    except Exception as error:
        print("Error!!!")
        Notify.SendMessageToLineNotify(error,Line_Notify_Token)
