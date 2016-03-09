import time
import json
import util
import redis
import inspect
import logging

from redis.exceptions import RedisError

'''
This script provides a Redis connection library 

Assumption:
-- All required databases are specified in database.json
'''

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class UnavailableDataError(RedisError):
      pass

class DBConnector(object):

    # Default values
    LOCAL_HOST = '127.0.0.1'

    REDIS_PORT = 6379

    # Wait period in seconds before retry connecting to Redis
    CONNECT_RETRY_WAIT_TIME = 10

    # Wait period in seconds before retry retrieving data from Redis
    DATA_RETRIEVAL_WAIT_TIME = 10

    # In Redis, by default keyspace events notifications are disabled because while not
    # very sensible the feature uses some CPU power. Notifications are enabled using
    # the notify-keyspace-events of redis.conf or via the CONFIG SET.
    # In order to enable the feature a non-empty string is used, composed of multiple characters,
    # where every character has a special meaning according to the following table:
    # K - Keyspace events, published with __keyspace@<db>__ prefix.
    # E - Keyevent events, published with __keyevent@<db>__ prefix.
    # g - Generic commands (non-type specific) like DEL, EXPIRE, RENAME, ...
    # $ - String commands
    # l - List commands
    # s - Set commands
    # h - Hash commands
    # z - Sorted set commands
    # x - Expired events (events generated every time a key expires)
    # e - Evicted events (events generated when a key is evicted for maxmemory)
    # A - Alias for g$lshzxe, so that the "AKE" string means all the events.
    # ACS Redis db mainly uses hash, therefore h is selected.
    KEYSPACE_EVENTS = 'KEgh'

    # A list of databases that are created or to be created on Redis
    db_map = {}

    def __init__(self):

        super(DBConnector, self).__init__()

        # For thread safety as recommended by python-redis
        # Create a separate client for each database
        self.redis_client = {}

        # Create a channel for receiving needed keyspace event
        # notifications for each client
        self.keyspace_notification_channel = {}

    @staticmethod
    def setup():

        DBConnector.load_database_map()

    @staticmethod
    def load_database_map():
        '''
            Get database map from an external json file
        '''

        db_file = util.get_full_path('data/database.json')
        with open(db_file, 'r') as file:
            DBConnector.db_map = json.load(file)

    @staticmethod
    def list_databases():
        '''
            Retrieve the list of databses
        '''

        return DBConnector.db_map

    @staticmethod
    def get_dbid(db_name):
        '''
            Retrive the database ID based on its name
        '''

        return DBConnector.db_map.get(db_name)

    def connect(self, db_name, retry_on=True):
        '''
            Connect to Database %db_name

            retry_on indicates whether to reconnect in case of of failure
            By default, retry_on is set to True
        '''

        if retry_on:
            self._persistent_connect(db_name)
        else:
            self._onetime_connect(db_name)

    def _onetime_connect(self, db_name):
        '''
            Connect to Database %db_name without retry upon failure
        '''

        db_id = self.get_dbid(db_name)
        client = redis.StrictRedis(host=DBConnector.LOCAL_HOST,
                                   port=DBConnector.REDIS_PORT,
                                   db=db_id)
        # Enable the notification mechanism for keyspace events in Redis
        client.config_set('notify-keyspace-events', DBConnector.KEYSPACE_EVENTS)
        self.redis_client[db_name] = client
        self._subscribe_keyspace_notification(db_name, client)

    def _persistent_connect(self, db_name):
        '''
            Keep reconnecting to Database %db_name until success
        '''

        while True:
            try:
                self._onetime_connect(db_name)
                return
            except RedisError:
                t_wait = DBConnector.CONNECT_RETRY_WAIT_TIME
                logger.warning('Connecting to DB %s failed, will retry in %s seconds'
                                    %(db_name, t_wait))
                self.close(db_name)
                time.sleep(t_wait)

    def close(self, db_name):

        self.redis_client[db_name].close()
        self.keyspace_notification_channel[db_name].close()


    def _subscribe_keyspace_notification(self, db_name, client):
        '''
            Subscribe the chosent client to keyspace event notifications
        '''

        pubsub = client.pubsub()
        pubsub = client.pubsub()
        pubsub.psubscribe('__key*__:*')
        self.keyspace_notification_channel[db_name] = pubsub


    def get_redis_client(self, db_name):

        return self.redis_client[db_name]

    def handle_unavailable_data(self, db_name, data):
        '''
            When the queried data is not available in Redis
            wait until it is available
        '''

        for message in self.keyspace_notification_channel[db_name].listen():
            if message.get('data') == data:
                # Wait for bulk data update before retrieval
                time.sleep(DBConnector.DATA_RETRIEVAL_WAIT_TIME)
                return

    def handle_connection_error(self, db_name):
        '''
            Reconnect to Redis server in case of connection failure
        '''

        time.sleep(DBConnector.CONNECT_RETRY_WAIT_TIME)
        self.close(db_name)
        self.connect(db_name, True)

    def keys(self, db_name, blocking):
        '''
            Retrieve all the keys of DB %db_name
        '''

        while True:
            try:
                return self._keys(db_name)
            except UnavailableDataError:
                if blocking:
                    self.handle_unavailable_data(db_name, 'hset')
                else:
                    return
            except RedisError:
                logger.warning('Connection to Redis failed, will retry soon...')
                if blocking:
                    self.handle_connection_error(db_name)
                else:
                    return

    def _keys(self, db_name):
        '''
            Wrapper function of Redis keys command
            Retrieve all the key in DB %db_name
            - raise exception if the databse is empty
        '''

        client = self.redis_client.get(db_name)
        keys = client.keys()
        if keys is None:
            logger.warning('DB %s has not data, keep waiting...' % db_name)
            raise UnavailableDataError('DB %s has not data, keep waiting...' % db_name)
        else:
            return keys


    def get(self, db_name, hash, key, blocking):
        '''
            Retrieve the value of Key %key from Hashtable %hash
            in Database %db_name

            Parameter %blocking indicates whether to wait
            when the query fails
        '''

        while True:
            try:
                return self._hget(db_name, hash, key)
            except UnavailableDataError:
                if blocking:
                    self.handle_unavailable_data(db_name, key)
                else:
                    return
            except RedisError:
                logger.warning('Connection to Redis failed, will retry soon...')
                if blocking:
                    self.handle_connection_error(db_name)
                else:
                    return

    def _hget(self, db_name, hash, key):
        '''
            Wrapper function for Redis command hget
            Retrieve the value of Key %key 
            - raise exception  when required key does not exist
        '''

        client = self.redis_client.get(db_name)
        val = client.hget(hash, key)
        if val is None:
            logger.warning('Key %s unavailable in Hashtable %s -  DB %s, keep waiting...' \
                            %(key, hash, db_name))
            raise UnavailableDataError('Key %s unavailable in Hashtable %s -  DB %s, keep waiting...'\
                                        %(key, hash, db_name))
        else:
            return val
    
    def get_all(self, db_name, hash, blocking):
        '''
            Get Hashtable %hash from DB %db_name

            Parameter %blocking indicates whether to wait 
            if the hashtable has not been created yet
        '''

        while True:
            try:
                return self._hgetall(db_name, hash)
            except UnavailableDataError:
                if blocking:
                    self.handle_unavailable_data(db_name, hash)
                else:
                    return
            except RedisError:
                logger.warning('Connection to Redis failed, will retry soon...')
                if blocking:
                    self.handle_connection_error(db_name)
                else:
                    return

    def _hgetall(self, db_name, hash):
        '''
            Wrapper function for Redis command hgetall
            Retrieve Hashtable %hash from DB %db_name
            - raise exception if the hashtable has not been created yet
        '''

        client = self.redis_client[db_name]
        table = client.hgetall(hash)
        if table is None:
            logger.warning('Hashtable %s does not exist, keep waiting...' % hash)
            raise UnavailableDataError('Hashtable %s does not exist in DB %s'\
                                        % (hash, db_name))
        else:
            return table

    def set(self, db_name, hash, key, val, blocking):
        '''
            Add %(key, val) to Hashtable %hash in DB %db_name
            Parameter %blocking indicates whether to retry in case of failure
        '''

        while True:
            try:
                return self._set(db_name, hash, key, val)
            except RedisError:
                logger.warning('Connection to Redis failed, will retry soon...')
                if blocking:
                    self.handle_connection_error(db_name)
                else:
                    return

    def _set(self, db_name, hash, key, val):
        '''
            Wrapper function for Redis command set
            Add (key, val) to the database
        '''

        client = self.redis_client[db_name]
        client.hset(hash, key, val)
