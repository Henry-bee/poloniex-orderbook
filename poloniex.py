from client import Pusher
import os, sys, datetime, time
from client.Utils import getLogger, SYMBOL2CODE
import numpy as np

channel_list = []
maxNbLimit = 0
exchCode = 'POLONIEX'

logger = getLogger()


class Book():
    def __init__(self, symbol, exchCode):
        self.symbol = symbol
        self.exchCode = exchCode
        self.bids = []
        self.asks = []
        self.bidSizes = []
        self.askSizes = []
        self.ltp = 0.0
        self.lts = 0.0
        self.trade_id = ''

    def bid(self, limit = 0):
        if len(self.bids) == 0:
            return 0
        return self.bids[limit]

    def ask(self, limit = 0):
        if len(self.asks) == 0:
            return 0
        return self.asks[limit]

    def bidSize(self, limit = 0):
        if len(self.bidSizes) == 0:
            return 0
        return self.bidSizes[limit]

    def askSize(self, limit = 0):
        if len(self.askSizes) == 0:
            return 0
        return self.askSizes[limit]

    def onQuote(self, limit, isBid, price, qty, ts, notify):
        import numpy as np
        update = []
        prev_bid = self.bid()
        prev_ask = self.ask()
        if isBid:
            while len(self.bids) <= limit:
                self.bids.append(0)
            while len(self.bidSizes) <= limit:
                self.bidSizes.append(0)
            if np.not_equal(self.bids[limit],float(price)) or np.not_equal(self.bidSizes[limit],float(qty)):
                update.append((ts, 'B', limit, float(price), float(qty)))
            self.bids[limit] = float(price)
            self.bidSizes[limit] = float(qty)
        else:
            while len(self.asks) <= limit:
                self.asks.append(0)
            while len(self.askSizes) <= limit:
                self.askSizes.append(0)
            if np.not_equal(self.asks[limit],float(price)) or np.not_equal(self.askSizes[limit],float(qty)):
                update.append((ts, 'A', limit, float(price), float(qty)))
            self.asks[limit] = float(price)
            self.askSizes[limit] = float(qty)

    def onTrade(self, price, qty, trade_id, ts):
        if self.trade_id != trade_id:
            self.ltp = price
            self.lts = qty
            self.trade_id = trade_id

    def snapshot(self, ts):
        update = []
        for limit in range(len(self.bids)):
            update.append((ts, 'B', limit, self.bid(limit), self.bidSize(limit)))
        for limit in range(len(self.asks)):
            update.append((ts, 'A', limit, self.ask(limit), self.askSize(limit)))

    def clear(self, bidLimit, askLimit):
        while len(self.bids) > bidLimit:
            self.bids.pop()
        while len(self.bidSizes) > bidLimit:
            self.bidSizes.pop()
        while len(self.asks) > askLimit:
            self.asks.pop()
        while len(self.askSizes) > askLimit:
            self.askSizes.pop()

    def printBook(self):
        msg = 'symbol : %s exchCode : %s nb bids : %d nb asks : %d ltp : %f lts : %f\n' % (self.symbol, self.exchCode, len(self.bids), len(self.asks), self.ltp, self.lts)
        for i in range(5):
            msg += '%f %f | %f %f\n' % (self.bidSize(i), self.bid(i), self.ask(i), self.askSize(i))
        logger.debug(msg)


books = {}

def orderRemoval(datafeed, book, isBid):
    '''
    Remove Order from Book if it exists
    Returns updated list of tuples (price, qty) from Book instance
    '''
    if isBid:
        # Remove Bid if exists
        bid_price = float(datafeed[2])
        length = len(book.bids)
        for i in range(0, length):
            if book.bids[i] ==  bid_price:
                book.bids.pop(i)
                book.bidSizes.pop(i)
        return list(zip(book.bids, book.bidSizes))

    else:
        # Remove asks if exists
        ask_price = float(datafeed[2])
        length = len(book.asks)
        for i in range(0, length):
            if book.asks[i] == ask_price:
                book.asks.pop(i)
                book.askSizes.pop(i)

        return [(x,y) for x,y in zip(book.asks, book.askSizes)]


def updateBidAsk(datafeed, book, isAsk, ts, notify=False):
    '''
    Update the bid/ask in book, and return list of bids/asks tuples

    Params:
    datafeed: JSON output from api
    book: an instance of Book class
    '''
    global maxNbLimit

    rate = float(datafeed[2])
    amt = float(datafeed[3])

    if isAsk:
        # ASKS
        askData = list(zip(list(book.asks), list(book.askSizes)))
        askData.append((rate, amt))

        if len(askData) > maxNbLimit:
            askData = sorted(askData, key=lambda x:x[0])[:maxNbLimit]
        else:
            askData = sorted(askData, key=lambda x:x[0])
        
        # Set asks and askSizes in Book
        askLimit = 0
        for price, qty in askData:
            book.onQuote(askLimit, False, price, qty, ts, notify)
            askLimit += 1
        return askData  
    else:
        # Bids
        bidData = list(zip(list(book.bids), list(book.bidSizes)))
        bidData.append((rate, amt))
        
        if len(bidData) > maxNbLimit:
            bidData = sorted(bidData, key=lambda x:x[0], reverse=True)[:maxNbLimit]
        else:
            bidData = sorted(bidData, key=lambda x:x[0], reverse=True)

        # Set bid and bidSize in Book
        bidLimit = 0
        for price, qty in bidData:
            book.onQuote(bidLimit, True, price, qty, ts, notify)
            bidLimit += 1
        return bidData


def onUpdate(tick, symbol):
    
    global maxNbLimit
    now = datetime.datetime.now()
    ts = (now - now.replace(hour=0,minute=0,second=0,microsecond=0)).total_seconds()
    logger.info('im here')
    
    if symbol in books.keys():
        book = books[symbol]
    else:
        book = Book(exchCode, symbol)
        books[symbol] = book

    if str(tick[0]) == 'i':
        # Initial snapshot
        notify = True

        # 5 Lowest Asks
        asks = tick[-1]['orderBook'][0].items()
        asks = [(float(k), float(v)) for k, v in asks]
        asks =  sorted(asks, key=lambda x: x[0])[:maxNbLimit]
        
        # 5 Highest Bids
        bids = tick[-1]['orderBook'][1].items()
        bids = [(float(k), float(v)) for k, v in bids]
        bids = sorted(bids, key=lambda x: x[0], reverse=True)[:maxNbLimit]
        
        logger.info('onUpdate %s Bids %s %s'% (exchCode, symbol, str(bids)))
        logger.info('onUpdate %s Asks %s %s'% (exchCode, symbol, str(asks)))

        bidLimit, askLimit = 0, 0
        # Inserting Ask Prices
        for price, qty in asks:
            book.onQuote(askLimit, False, price, qty, ts, notify)
            askLimit += 1
        # Inserting Bid Prices
        for price, qty in bids:
            book.onQuote(bidLimit, True, price, qty, ts, notify)
            bidLimit += 1
        book.clear(bidLimit, askLimit)
        book.snapshot(ts)
        
    if str(tick[0]) == 't':
        # Trade Event

        tradeID = tick[1]
        rate = float(tick[3])
        amt = float(tick[4])
        logger.info('onUpdate %s TRADES %s %s'% (exchCode, symbol, str(tick)))
        book.onTrade(rate, amt, tradeID, ts)
        book.snapshot(ts)

    if str(tick[0]) == 'o':
        if str(tick[-1]) == "0.00000000":
            # Order Removal
            if tick[1] == 0:
                # ASKS
                askData = orderRemoval(tick, book, False)
                logger.info('onUpdate %s Asks %s %s'% (exchCode, symbol, str(askData)))
                
            else:
                # BIDS
                bidData = orderRemoval(tick, book, True)
                logger.info('onUpdate %s Bids %s %s'% (exchCode, symbol, str(bidData)))
                
        else:
            # New Order
            if tick[1] == 0:
                askData = updateBidAsk(tick, book, True, ts)
                logger.info('onUpdate %s Asks %s %s'% (exchCode, symbol, str(askData)))
            else:

                bidData = updateBidAsk(tick, book, False, ts)
                logger.info('onUpdate %s Bids %s %s'% (exchCode, symbol, str(bidData)))
        book.snapshot(ts)


    '''     
    #start writing info.
    if channel_name.startswith('order_book'):
        bidData = data['bids'][:maxNbLimit]
        askData = data['asks'][:maxNbLimit]
        logger.info('onUpdate %s Bids %s %s'% (exchCode, channel_name.split('_')[-1], str(bidData)))
        logger.info('onUpdate %s Asks %s %s'% (exchCode, channel_name.split('_')[-1], str(askData)))
        notify = True
        if len(book.bids) == 0:
            notify = False
        bidLimit, askLimit = 0, 0
        for update in bidData:
            price, qty = float(update[0]), float(update[1])
            book.onQuote(bidLimit, True, price, qty, ts, notify)
            bidLimit += 1
        #doesnt need to reverse it because the original format is good
        for update in askData:
            price, qty = float(update[0]), float(update[1])
            book.onQuote(askLimit, False, price, qty, ts, notify)
            askLimit += 1
        book.clear(bidLimit, askLimit)
        # initial book
        if not notify:
            book.snapshot(ts)
    elif channel_name.startswith('live_trades'):
        logger.info('onUpdate %s TRADES %s %s'% (exchCode, channel_name.split('_')[-1], str(data)))
        price = float(data['price'])
        book.onTrade(data['price'],data['amount'],data['id'],ts)
    else:
        logger.error('onUpdate unsupported message %s: %s' % (channel_name, data))
    book.printBook()
    return
    '''

def onConnect():
    global socket
    logger.info('POLONIEX onConnect')
    channel_list = ['USDT_BTC', 'BTC_LTC']

    for channel_name in channel_list:
        channel = socket.subscribe(channel_name)
        code = SYMBOL2CODE[channel_name]
        channel.bind(code, onUpdate, kwargs={'symbol':channel_name})


def onReconnect():
    logger.info('POLONIEX onReconnect')

def onDisconnect():
    logger.info('POLONIEX onDisconnect')

def onConnectError(error):
    logger.error('POLONIEX onConnectError %s' % error)

def start():
    global socket
    global maxNbLimit
    # global nbOfEmptyData
    maxNbLimit = 5
    socket = Pusher(onUpdate)
    socket.connection.setBasicListener(onConnect, onDisconnect, onConnectError, onReconnect)
    socket.connect()
    while True:
        time.sleep(1)

def main():
    logger.info('Main Start!')
    start()

if __name__ == '__main__':
    main()
