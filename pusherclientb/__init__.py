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

import numpy as np
from connection import Connection
from book import Book
from utils import getInitialSnapshot, buildMap, updateBidAsk, logTrade, logBidAsk, orderRemoval
import requests as re

from log import getLogger


# Data logger
dataLogger = getLogger('DataLogger')

# Server Logger
serverLogger = getLogger('ServerLogger')

maxNbLimit = 5

# Notify in onQuote?
notify =False

class PoloniexPush(object):

    def __init__(self):

        self.SYMBOL_CODE = buildMap()
        self.books = {self.SYMBOL_CODE[key]: Book(self.SYMBOL_CODE[key], 'Poloniex') for key in self.SYMBOL_CODE}
        self.connection = Connection(self.onUpdate, self.onConnect, serverLogger)

    def onConnect(self, ws):
        '''Initial websocket handshake is complete'''

        serverLogger.info("onConnect")

        for symbol in self.books:
            payload=json.dumps({'command':'subscribe', 'channel':symbol})
            ws.send(payload)

        ws.send(payload)
        serverLogger.info("Subscribing to %s channels"%(len(self.books)))

        # Initialize Snapshot
        getInitialSnapshot(maxNbLimit, self.books, serverLogger)
        


    def orderBookEvent(self, tick, SYMBOL):
        '''
        orderBookEvent 
        '''

        TYPE = 'ASK' if tick[1] == 0 else 'BID'

        # orderBook Removal
        if str(tick[-1]) == "0.00000000": 

            if TYPE == 'ASK':

                orderRemoval(tick, self.books[SYMBOL], False)
                logBidAsk(self.books[SYMBOL], False, dataLogger)    

            else:

                orderRemoval(tick, self.books[SYMBOL], True)
                logBidAsk(self.books[SYMBOL], True, dataLogger)
            
        else:
            # orderBook Modify
        
            if TYPE == 'ASK':
    
                # UpdateBidAsk in Book
                updateBidAsk(tick, self.books[SYMBOL], True, notify)                            
                # LogBidAsk as requested
                logBidAsk(self.books[SYMBOL], False, dataLogger)

            else:
                
                updateBidAsk(tick, self.books[SYMBOL], False, notify)
                logBidAsk(self.books[SYMBOL], True, dataLogger) 

    def tradeEvent(self, tick, SYMBOL):

        isBuy = False if tick[2] == 0 else True
        tradeID = tick[1]
        rate = float(tick[3])
        amt = float(tick[4])
        ts = str(tick[-1])

        self.books[SYMBOL].onTrade(rate, amt, tradeID, ts)
        logTrade(self.books[SYMBOL], isBuy, dataLogger, ts)

    def onUpdate(self, ws, message):
        # Run everytime server emits message

        res = json.loads(message)

        if 'error' in res:
            serverLogger.error(res['error'])
            return

        if res[0] == 1010:
            #Heartbeat
            self.connection.connected = True
            return
        
        # Channel Identity
        symCode = str(res[0])

        self.connection.connected = True
 
        try:
            SYMBOL = self.SYMBOL_CODE[symCode]
        except KeyError as e:
            serverLogger.error("no mapping for code %s"%(symCode))

            return
        
        ticks = res[-1]

        for tick in ticks:
            
            if str(tick[0]) == 'o':
                self.orderBookEvent(tick, SYMBOL)
                    
            if str(tick[0]) == 't':
                
                self.tradeEvent(tick, SYMBOL)

    
    def run(self):
        self.connection.start()

