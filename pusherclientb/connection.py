import websocket
from threading import Thread
import time

# This is the API that Polononiex front-end is subscribed to,
# Hidden from public
URL_PATH = "wss://api2.poloniex.com/"

class Connection(object):
    def __init__(self, onUpdate, onConnect, logger):
        '''
        onUpdate: self defined function on how websocket should respond on new data
        '''
        self.onConnect = onConnect
        self.onUpdate = onUpdate

        self.socket = websocket.WebSocketApp(
            URL_PATH,
            on_open = self.onConnect,
            on_message= self.onUpdate,
            on_error= self.onConnectError,
            on_close= self.onDisconnect
            )
        
        self.serverLogger = logger
        self.connected = True

    def onConnectError(self, ws, error):
        # Only happen when first connection is not successful
        # Or KeyBoardInterrupt/SystemExit
        # Or Ping/Pong timeout => WebSocketTimeOutException (Derived Class of Exception) 
        # will be raised in run_forever, and this function will be called
        self.serverLogger.error('onConnectError '+ error)
        self.connected = False

        # Explicitly close both the socket and thread
        self.socket.close()
        if self.thread.isAlive(): self.thread.join()
        

    def onDisconnect(self, ws):
        # Happens when there is an unexpected outage
        # Will attempt reconnection
        self.serverLogger.error('onDisconnect')
        self.connected = False

        # Explicitly close both the socket and thread
        self.socket.close()
        if self.thread.isAlive(): self.thread.join()


    def reconnect(self):
        self.serverLogger.info("Reconnecting")

        #Ensure both socket and thread are dead
        self.socket.close()
        self.thread.join()

        self.socket = websocket.WebSocketApp(
            URL_PATH,
            on_open = self.onConnect,
            on_message= self.onUpdate,
            on_error= self.onConnectError,
            on_close= self.onDisconnect
            )

        self.connected = False

        self.start()

    def start(self):

        self.thread = Thread(target=self.socket.run_forever)

        # Setting it to daemon allow thread to be killed once user exit program
        self.thread.daemon = True
        self.thread.start()

        time.sleep(10)

        while True:
            # Ensure thread stays alive, reconnect otherwise every 5 seconds
            time.sleep(5)
            
            if not self.connected: 
                self.serverLogger.info("Service is disrupted, attempting to reconnect")
                return self.reconnect()