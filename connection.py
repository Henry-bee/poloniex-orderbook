import websocket
import ssl
import Utils
import time
import json
import logging
import numpy as np
from Utils import CODE2SYMBOL

logging.basicConfig(filename='.PLNXWebsocketError.txt', level=logging.DEBUG, format='%(asctime)s.%(msecs)03d - %(levelname)s - [ %(message)s ] - (%(filename)s:%(lineno)s||T=%(thread)d)', datefmt='%H:%M:%S')

log = Utils.getLogger(loggerName='PLNXConnection',logLevel='INFO')

class orderBook(object):
    max_size = 5
    def __init__(self, symbol):
        self.symbol = ''.join(symbol.split('_'))
        self.bids = []
        self.asks = []
        self.bidSizes = []
        self.askSizes = []
        self.ltp = None # Price
        self.lts = None # Size
        self.trade_id = '' # ID

    def snapAsks(self, askData):
        '''
        Initial Snapshot: Sort and Append asks Data to book object
        '''
        asks = askData.items()
        asks = [(float(k), float(v)) for k, v in asks]
        asks =  sorted(asks, key=lambda x: x[0])[:self.max_size]

        for price, qty in asks:
            self.asks.append(price)
            self.askSizes.append(qty)
        return self.asks,  self.askSizes

    def snapBids(self, bidData):
        '''
        Initial Snapshot: Sort and append bids Data to Book object
        '''
        bids = bidData.items()
        bids = [(float(k), float(v)) for k, v in bids]
        bids = sorted(bids, key=lambda x: x[0], reverse=True)[:self.max_size]
        
        for price, qty in bids:
            self.bids.append(price)
            self.bidSizes.append(qty)
        return self.bids, self.bidSizes

    def orderRemoval(self, datafeed, isBid):
        '''
        Remove Order from Book if it exists
        Returns updated list of tuples (price, qty) from Book instance
        '''
        if isBid:
            # Remove Bid if exists

            newbids = []
            newBidsize = []
            bid_price = float(datafeed[2])
            for bid, qty in zip(self.bids, self.bidSizes):
                if bid == bid_price: continue
                else:
                    newbids.append(bid)
                    newBidsize.append(qty)

            if len(newbids) < 5:
                short = 5 - len(newbids)
                extension = [0.0] * short
                newbids.extend(extension)
                newBidsize.extend(extension)

            self.bids = newbids
            self.bidSizes = newBidsize
            
        else:
            # Remove asks if exists
            
            newAsks = []
            newAsksize = []
            ask_price = float(datafeed[2])
            for ask, qty in zip(self.asks, self.askSizes):
                if ask == ask_price: continue
                else:
                    newAsks.append(ask)
                    newAsksize.append(qty)

            if len(newAsks) < 5:
                short = 5 - len(newAsks)
                extension = [0.0] * short
                newAsks.extend(extension)
                newAsksize.extend(extension)
                
            self.asks = list(newAsks)
            self.askSizes = list(newAsksize)
            

    def updateTrade(self, rate, amt, id):
        self.ltp = rate
        self.lts = amt
        self.trade_id = id

    def updateBidAsk(self, datafeed, isAsk):
        '''
        Update the bid/ask in book
        '''
        rate = float(datafeed[2])
        amt = float(datafeed[3])

        if isAsk:
            # ASKS
            askData = list(zip(list(self.asks), list(self.askSizes)))
            
            askData.append((rate, amt))

            # Sort and filter
            if len(askData) > self.max_size:
                askData = sorted(askData, key=lambda x:x[0])[:self.max_size]
            else:
                askData = sorted(askData, key=lambda x:x[0])
            
            self.asks = [ask[0] for ask in askData]
            self.askSizes = [ask[1] for ask in askData]

        else:
            # Bids
            bidData = list(zip(list(self.bids), list(self.bidSizes)))
            bidData.append((rate, amt))
            
            # Sort and filter
            if len(bidData) > self.max_size:
                bidData = sorted(bidData, key=lambda x:x[0], reverse=True)[:self.max_size]
            else:
                bidData = sorted(bidData, key=lambda x:x[0], reverse=True)

            self.bids = [x[0] for x in bidData]
            self.bidSizes = [x[1] for x in bidData]

class PLNXConnection():
    url = "wss://api2.poloniex.com/"
    def __init__(self, symbols, onUpdate):
        self.socket = self.onConnected = self.onReconnected = self.onDisconnected = self.onConnectError = None
        self.onUpdate = onUpdate
        self.disconnect_called = False
        self.needs_reconnect = False
        self.default_reconnect_interval = 3
        self.reconnect_interval = 3
        self.books = {symbol: orderBook(symbol) for symbol in symbols}
        self.symbols = symbols

    def setBasicListener(self, onConnected, onDisconnected, onConnectError, onReconnected):
        self.onConnected = onConnected
        self.onDisconnected = onDisconnected
        self.onConnectError = onConnectError
        self.onReconnected = onReconnected

    def disconnect(self):
        self.needs_reconnect = False
        self.disconnect_called = True
        if self.socket:
            self.socket.close()

    def reconnect(self, reconnect_interval=None):
        if self.onReconnected is not None:
            self.onReconnected()
        if reconnect_interval is None:
            reconnect_interval = self.default_reconnect_interval
        log.info("Connection: Reconnect in %s" % reconnect_interval)
        self.reconnect_interval = reconnect_interval
        self.needs_reconnect = True
        if self.socket:
            self.socket.close()

    def connect_Socket(self):
        websocket.enableTrace(False)
        self.socket = websocket.WebSocketApp(self.url,
                                             on_message=self._on_message,
                                             on_error=self._on_error,
                                             on_close=self._on_close,
                                             on_open=self._on_open)
        while True:
            try:
                self.socket.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

                break
            except Exception, err:
                log.error("The Following error occured: %s" % (err))

        while self.needs_reconnect and not self.disconnect_called:
            log.info("Attempting to connect again in %s seconds."% self.reconnect_interval)
            time.sleep(self.reconnect_interval)
            # We need to set this flag since closing the socket will set it to
            # false
            self.socket.keep_running = True
            self.socket.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


    def _on_message(self, ws, message):
        exchData = json.loads(message)

        if exchData[0] == 1010: return

        symbol = CODE2SYMBOL[str(exchData[0])]
        book = self.books[symbol]

        ticks = exchData[-1]
        
        for tick in ticks:

            if str(tick[0]) == 'i':
                asks, askSizes = book.snapAsks(tick[-1]['orderBook'][0])
                bids, bidSizes = book.snapBids(tick[-1]['orderBook'][1])

                self.onUpdate([zip(bids, bidSizes),zip(asks, askSizes)],'orderbook_' + book.symbol)

            if str(tick[0]) == 'o':
                if str(tick[-1]) == "0.00000000":
                    # Order Removal
                    
                    if tick[1] == 0:
                        # ASKS
                        book.orderRemoval(tick, False)
  
                    else:
                        # BIDS
                        book.orderRemoval(tick, True)
 
                else:
                    # NEW ORDER
                    if tick[1] == 0:
                        # ASKS
                        book.updateBidAsk(tick, True)
                    else:
                        # BIDs
                        book.updateBidAsk(tick, False)

                bids, bidSizes = list(book.bids), list(book.bidSizes)
                asks, askSizes = list(book.asks), list(book.askSizes)
                self.onUpdate([zip(bids, bidSizes),zip(asks, askSizes)],'orderbook_' + book.symbol)

            if str(tick[0]) == 't':
                tradeID = tick[1]
                rate = float(tick[3])
                amt = float(tick[4])
                book.updateTrade(rate, amt, tradeID)
                self.onUpdate([rate,amt,tradeID], 'tradebook_' + book.symbol)

    def _on_error(self, ws, error):
        log.info("Connection: Error - %s" % error)
        if self.onConnectError is not None:
            self.onConnectError(error)
        self.needs_reconnect = True


    def _on_close(self, ws):
        log.info("Connection: Connection closed")
        if self.onDisconnected is not None:
            self.onDisconnected()


    def _on_open(self, ws):
        log.info("Connection: Connection opened")

        if self.onConnected is not None:
            self.onConnected()

        for symbol in self.symbols:
            ws.send(json.dumps({'command':'subscribe', 'channel':symbol}))

