'''
1. live streaming order book level data
2. handle live data error, such as connection lost, or delay
3. easy to call the error free data for other functions /program/ scripts 
easier for me to check and fit in my existing program.


Output two log files:
1: data file consisting of order book level data
2: server log file to track server status
'''

import json
import logging
import time
from datetime import datetime
from logging import Formatter
from threading import Thread

import requests as re

import websocket

SYMBOL ='USDT_BTC' # QUOTE_BASE
quoteCurr, baseCurr = SYMBOL.split('_')

# Poloniex Server API is known to be rather unstable
# Hence, wise to track server time vs local machine time
# https://www.reddit.com/r/BitcoinMarkets/comments/6h4mc9/poloniex_push_api_woes/
serverLagCheck = False

LOGFORMAT = Formatter("%(asctime)s : %(message)s")
LOGLEVEL = logging.INFO

# Data logger
dataLogger = logging.getLogger('DataLogger')
dataLogger.setLevel(LOGLEVEL)
data_path = './poloniex_disrupted_data.log'
data_file_handler = logging.FileHandler(data_path)
data_file_handler.setFormatter(LOGFORMAT)
dataLogger.addHandler(data_file_handler)


# Server Logger
serverLogger = logging.getLogger('ServerLogger')
serverLogger.setLevel(LOGLEVEL)
server_path ='./poloniex_disrupted_server.log'
server_file_handler = logging.FileHandler(server_path)
server_file_handler.setFormatter(LOGFORMAT)
serverLogger.addHandler(server_file_handler)



class PoloBook(object):

    def __init__(self):

        self.socket = websocket.WebSocketApp(
            "wss://api2.poloniex.com:443",
            on_open = self.onConnect,
            on_message= self.onUpdate,
            on_error= self.onConnectError,
            on_close= self.onDisconnect
            )

        #Flag to check if thread is running
        self.connected = False

    def onConnect(self, ws):
        '''Initial websocket handshake is complete'''
        serverLogger.info("onConnect")
        payload=json.dumps({'command':'subscribe', 'channel':SYMBOL})
        ws.send(payload)


    def onUpdate(self, ws, message):
        # Run everytime server emits message
        res = json.loads(message)

        if 'error' in res:
            serverLogger.error(res['error'])
            return
        
        ticks = res[-1]

        self.connected = True

        for tick in ticks:

            if str(tick[0]) == 'o':
                # Order information
                # [u'o', 0, u'16873.50700000', u'0.00000000']
                # 0 - Sell, 1- Buy

                ORDER = 'SELL' if tick[1] == 0 else 'BUY'
                datafeed = "%s Order at Price:%s (%s), Quantity %s:%s"%(ORDER, tick[2], quoteCurr, baseCurr, tick[-1])

                dataLogger.info(datafeed)


            if str(tick[0]) == 't':
                # API occasionally spits timestamp mixed with datafeed
                # Good way to check tardiness of datafeed
                # [u't', u'15275994', 1, u'16260.00000000', u'0.03139079', 1513243540]

                ORDER =  'SELL' if tick[2] == 0 else 'BUY'
                datafeed = "%s Order at Price:%s (%s), Quantity %s:%s"%(ORDER, tick[3], quoteCurr, baseCurr, tick[-2])

                dataLogger.info(datafeed)

                # Output time difference between API time and local machine time to console
                if serverLagCheck:
                    servertime = tick[-1]
                    localtime = time.time()

                    lag = (localtime - servertime) / 60

                    time_now = datetime.utcnow()

                    if lag > 1:
                        serverLogger.warn("Server lagging %s (>1min) Timestamp:%s"%(lag, time_now))


    def onConnectError(self, ws, error):
        # Only happen when first connection is not successful
        # Or KeyBoardInterrupt/SystemExit
        # Or Ping/Pong timeout => WebSocketTimeOutException (Derived Class of Exception) 
        # will be raised in run_forever, and this function will be called
        serverLogger.error('onConnectError '+ error)
        self.connected = False

        # Explicitly close both the socket and thread
        self.socket.close()
        if self.thread.isAlive(): self.thread.join()
        

    def onDisconnect(self, ws):
        # Happens when there is an unexpected outage
        # Will attempt reconnection
        serverLogger.error('onDisconnect')
        self.connected = False

        # Explicitly close both the socket and thread
        self.socket.close()
        if self.thread.isAlive(): self.thread.join()


    def reconnect(self):
        serverLogger.info("Reconnecting")

        #Ensure both socket and thread are dead
        self.socket.close()
        if self.thread.isAlive(): self.thread.join()

        self.socket = websocket.WebSocketApp(
            "wss://api2.poloniex.com:443",
            on_open = self.onConnect,
            on_message= self.onUpdate,
            on_error= self.onConnectError,
            on_close= self.onDisconnect
            )

        self.connected = False

        self.start()

    def start(self):

        self.thread = Thread(target=self.socket.run_forever, kwargs={'ping_timeout':2})

        # Setting it to daemon allow thread to be killed once user exit program
        self.thread.daemon = True
        self.thread.start()

        # Wait for initial handshake
        time.sleep(10)
        
        # Stress-test
        # t = 1
        while True:
            # Ensure thread stays alive, reconnect otherwise every 5 seconds
            
            time.sleep(5)
            
            if not self.connected: 
                serverLogger.info("Service is disrupted, attempting to reconnect")
                return self.reconnect()
            
            # Stress-Test
            #if t%2 == 0: 
            #    self.onDisconnect(self.socket)
            #t += 1

if __name__ == '__main__':      
    book = PoloBook()
    book.start()
