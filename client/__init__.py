from .channel import Channel
from .connection import Connection
import hashlib
import hmac
import logging
import json

class Pusher():
    url =  "wss://api2.poloniex.com/"
    def __init__(self, secure=True, secret=None, user_data=None, log_level=logging.INFO, daemon=True, port=None, reconnect_interval=10):
        # self.key = key
        self.secret = secret
        self.user_data = user_data or {}

        self.channels = {}

        self.connection = Connection(self._connection_handler, self.url, log_level=log_level, daemon=daemon, reconnect_interval=reconnect_interval)

    def connect(self):
        """Connect to Pusher"""
        self.connection.start()

    def disconnect(self):
        """Disconnect from Pusher"""
        self.connection.disconnect()
        self.channels = {}

    def subscribe(self, channel_name):
        """Subscribe to a channel

        :param channel_name: The name of the channel to subscribe to.
        :type channel_name: str

        :rtype : Channel
        """
        # data = {'channel': channel_name}
        # self.connection.send_event('pusher:subscribe', data)

        self.connection.send_event('subscribe', channel_name=channel_name)
        
        self.channels[channel_name] = Channel(channel_name, self.connection)

        return self.channels[channel_name]
    
    def unsubscribe(self, channel_name):
        """Unsubscribe from a channel

        :param channel_name: The name of the channel to unsubscribe from.
        :type channel_name: str
        """
        if channel_name in self.channels:
            self.connection.send_event('unsubscribe', channel_name=channel_name)
            del self.channels[channel_name]

    def channel(self, channel_name):
        """Get an existing channel object by name

        :param channel_name: The name of the channel you want to retrieve
        :type channel_name: str

        :rtype: Channel or None
        """
        return self.channels.get(channel_name)

    def _connection_handler(self, event_name, data, channel_name):
        if channel_name in self.channels:
            self.channels[channel_name]._handle_event(event_name, data)
