import time, datetime
import Utils
from connection import PLNXConnection
log = Utils.getLogger(loggerName='PLNXDataHandler',logLevel='INFO')

h5books = {}
exchCode = 'PLNX'


logger = Utils.getLogger()

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

    def bid(self, limit=0):
        if len(self.bids) == 0:
            return 0
        return self.bids[limit]

    def ask(self, limit=0):
        if len(self.asks) == 0:
            return 0
        return self.asks[limit]

    def bidSize(self, limit=0):
        if len(self.bidSizes) == 0:
            return 0
        return self.bidSizes[limit]

    def askSize(self, limit=0):
        if len(self.askSizes) == 0:
            return 0
        return self.askSizes[limit]

    def onQuote(self, limit, isBid, price, qty, ts):
        import numpy as np
        update = []
        if isBid:
            while len(self.bids) <= limit:
                self.bids.append(0)
            while len(self.bidSizes) <= limit:
                self.bidSizes.append(0)
            if np.not_equal(self.bids[limit], float(price)) or np.not_equal(self.bidSizes[limit], float(qty)):
                update.append((ts, 'B', limit, float(price), float(qty)))
            self.bids[limit] = float(price)
            self.bidSizes[limit] = float(qty)
        else:
            while len(self.asks) <= limit:
                self.asks.append(0)
            while len(self.askSizes) <= limit:
                self.askSizes.append(0)
            if np.not_equal(self.asks[limit], float(price)) or np.not_equal(self.askSizes[limit], float(qty)):
                update.append((ts, 'A', limit, float(price), float(qty)))
            self.asks[limit] = float(price)
            self.askSizes[limit] = float(qty)

    def onTrade(self, price, qty, trade_id, ts):
        if self.trade_id != trade_id:
            self.ltp = price
            self.lts = qty
            self.trade_id = trade_id
        logger.info("TradeBook Data  for symbol %s => The ltp is : %s , The lts is : %s , The Trade_id is : %s " % (self.symbol, self.ltp, self.lts, self.trade_id))

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
        msg = 'symbol : %s exchCode : %s nb bids : %d nb asks : %d ltp : %f lts : %f\n' % (
        self.symbol, self.exchCode, len(self.bids), len(self.asks), self.ltp, self.lts)
        for i in range(5):
            msg += '%.8f %.8f | %.8f %.8f\n' % (self.bid(i), self.bidSize(i), self.ask(i), self.askSize(i))
        logger.info(msg)

def onUpdate(data, channel_name, maxNbLimit=None):
    if type(data) is list:
        now = datetime.datetime.now()
        ts = (now - now.replace(hour=0,minute=0,second=0,microsecond=0)).total_seconds()
        symbol = channel_name.split('_')[-1]
        book = Book(symbol, exchCode)
        #start writing info.
        if channel_name.startswith('orderbook'):
            bidList = data[0]
            askList = data[1]
            bidLimit, askLimit =0,0
            for bid in bidList:
                book.onQuote(limit=bidLimit,isBid=True,price=bid[0],qty=bid[1],ts=ts)
                bidLimit += 1
            for ask in askList:
                book.onQuote(limit=askLimit,isBid=False,price=ask[0],qty=ask[1],ts=ts)
                askLimit += 1
            book.clear(bidLimit, askLimit)
        elif channel_name.startswith('tradebook'):
            book.onTrade(price=data[0],qty=abs(data[1]),trade_id=data[2],ts=ts)
        else:
            log.error('onUpdate unsupported message %s: %s' % (channel_name, data))
        book.printBook()
        return


def connect_handler(symbols):
    try:
        conn = PLNXConnection(symbols,onUpdate)
        conn.connect_Socket()
    except Exception, err:
        log.error('The following error has occured: %s' % (err))

def main(channels=''):
    channels = ['USDT_BTC']
    connect_handler(channels)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()