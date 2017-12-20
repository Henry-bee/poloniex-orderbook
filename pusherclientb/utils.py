import requests as re
import time
from book import Book
from datetime import datetime
from collections import OrderedDict

def buildMap():
    '''
    Returns a dictionary of mapping between
    '''
    result = re.get('https://poloniex.com/public?command=returnTicker')
    _json = result.json()

    return { str(_json[sym]['id']): sym for sym in _json}

def getInitialSnapshot(depth, book, logger):
    '''
    Obtain a snapshot of the OrderBook in its current state for all Pairs
    params:
    depth: # of bid/ask to track in book

    Return:
    Dictionary of Book instances of every symbol
    '''

    logger.info("Begin getInitialSnapShot")

    # Public API: Retrieve Order Book of all Currency Pairs
    res = re.get("https://poloniex.com/public", params={'command':'returnOrderBook', 'currencyPair':'all', 'depth':depth})
    orderBookResult = res.json()

    for sym in orderBookResult:
        datafeed = orderBookResult[sym]
        setBidAsk(datafeed, depth, book[sym])
        
    logger.info("Completed getInitialSnapshot")


def setBidAsk(datafeed, depth, book):
    '''
    Set bids, asks, bidSizes, askSizes in Book

    Params:
    datafeed: JSON output from api
    depth: maxNbLimit
    book: an instance of Book class
    '''

    # Rearrange bids and bidSizes in descending order
    bids = [(float(x[0]), float(x[1])) for x in datafeed['bids']]
    top_n_bid = sorted(bids, key= lambda x: x[0], reverse=True)[:depth]
    bids = [ bid[0] for bid in top_n_bid]
    bidSizes = [bid[1] for bid in top_n_bid]
    
    ts = time.time()

    # Set bid and bidSize in Book
    bidLimit = 0
    for price, qty in zip(bids, bidSizes):
        book.onQuote(bidLimit, True, price, qty, ts, False)
        bidLimit += 1

    # Rearrange asks and askSizes in ascending order
    asks = [ (float(x[0]), x[1]) for x in datafeed['asks']]
    bottom_n_asks = sorted(asks, key= lambda x: x[0])[:depth]
    asks = [ask[0] for ask in bottom_n_asks]
    askSizes = [ask[1] for ask in bottom_n_asks]

    # Set ask and askSizes in Book
    askLimit = 0
    for price, qty in zip(asks, askSizes):
        book.onQuote(askLimit, False, price, qty, ts, False)
        askLimit += 1

    

def updateBidAsk(datafeed, book, isAsk, notify=False):
    '''
    Returns bid/ask array, bid/ask size array including the new datafeed (trimmed and sorted)

    Params:
    datafeed: JSON output from api
    book: an instance of Book class
    '''

    rate = float(datafeed[2])
    amt = float(datafeed[3])
    ts = int(time.mktime(datetime.utcnow().timetuple()))

    
    if isAsk:
        # ASK

        length = len(book.asks)

        # Pythonic list copy
        asks = list(book.asks)
        askSizes = list(book.askSizes)

        for i in range(0, length):
            if rate < asks[i]:

                # Perform insertion at target index
                asks.insert(i, rate)
                askSizes.insert(i, amt)

                # Remove the last element
                asks.pop()
                askSizes.pop()

                break

        # Update ask and askSizes in Book
        askLimit = 0
        for price, qty in zip(asks, askSizes):
            book.onQuote(askLimit, False, price, qty, ts, notify)
            askLimit += 1

    else:
        # Bids

        length = len(book.bids)

        # Pythonic list copy
        bids = list(book.bids)
        bidSizes = list(book.bidSizes)

        for i in range(0, length):
            if rate > bids:
                
                # Perform insertion at target index
                bids.insert(i, rate)
                bidSizes.insert(i, amt)

                # Ramove the last element
                bids.pop()
                bidSizes.pop()

                break

        # Set bid and bidSize in Book
        bidLimit = 0
        for price, qty in zip(bids, bidSizes):
            book.onQuote(bidLimit, True, price, qty, ts, notify)
            bidLimit += 1

def orderRemoval(datafeed, book, isBid):
    '''
    Remove Order from Book
    '''
    if isBid:
        # Remove Bid if exists
        
        bid_price = float(datafeed[2])
        length = len(book.bids)
        
        for i in range(0, length):
            if book.bids[i] ==  bid_price:
                book.bids.pop(i)
                book.bidSizes.pop(i)

    else:
        # Remove asks if exists

        ask_price = float(datafeed[2])
        length = len(book.asks)

        for i in range(0, length):
            if book.asks[i] == ask_price:
                book.asks.pop(i)
                book.askSizes.pop(i)


def logBidAsk(book, isBid, logger):
    '''
    Dispatch Bid and Ask log from book object
    onUpdate BITS Bids btcusd [[u'19351.93000000', u'0.00333373'], [u'19351.92000000', u'0.22429271'], [u'19351.91000000', u'0.22800000'], [u'19351.90000000', u'0.03009107'], [u'19351.89000000', u'7.10927719']]
    '''

    if isBid:
        bidStack = [ [str(bid), str(size)] for bid, size in zip(book.bids, book.bidSizes)]
        message = "onUpdate %s Bids %s %s"%(book.exchCode, book.symbol, bidStack)
    else:
        askStack = [ [str(ask), str(size)] for ask, size in zip(book.asks, book.askSizes)]
        message = "onUpdate %s Asks %s %s"%(book.exchCode, book.symbol, askStack)
    
    logger.info(message)

def logTrade(book, isBuy, logger, ts):

    '''
    Dispatch Trade Log from book object 
    '''
    
    trade_dict = {
        'buy_order_id': '', 
        'sell_order_id': '', 
        'timestamp': '', 
        'price_str': '', 
        'id': '', 
        'amount': 0, 
        'amount_str': '',
        'type': 0, 
        'price': 0.0}

    if isBuy:

        trade_dict['buy_order_id'] = str(book.trade_id)

    else:
        trade_dict['sell_order_id'] = str(book.trade_id)

    trade_dict['id'] = str(book.trade_id)
    trade_dict['timestamp'] = str(ts)
    trade_dict['price'] = float(book.ltp)
    trade_dict['price_str'] = str(book.ltp)
    trade_dict['type'] = 0
    trade_dict['amount'] = float(book.lts)
    trade_dict['amount_str'] = str(book.lts)


    message = "onUpdate %s TRADES %s %s"%(book.exchCode, book.symbol, trade_dict)

    logger.info(message)
    
        
