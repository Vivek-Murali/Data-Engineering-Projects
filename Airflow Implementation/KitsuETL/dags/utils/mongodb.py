import pymongo
import os
from utils import keys
""" from dotenv import load_dotenv
load_dotenv() """

class Database(object):
    Key = keys.Keys()
    USERNAME = Key.DB_USERNAME
    PASSWORD = Key.DB_PASSWORD
    URI = Key.DB_URI
    database = Key.DB_NAME
    DATABASE = None

    @staticmethod
    def initialize():
        client = pymongo.MongoClient(Database.URI,username=Database.USERNAME,password=Database.PASSWORD)
        Database.DATABASE = client.anime

    @staticmethod
    def insert(collection, data):
        Database.DATABASE[collection].insert(data)

    @staticmethod
    def insert_many(collection, data):
        Database.DATABASE[collection].insert_many(data)

    @staticmethod
    def find(collection, query):
        return Database.DATABASE[collection].find(query)

    @staticmethod
    def find_one(collection, query):
        return Database.DATABASE[collection].find_one(query)

    @staticmethod
    def initializing_city(collection, data):
        Database.DATABASE[collection].insert(data)

    @staticmethod
    def update(collection, query):
        return Database.DATABASE[collection].update_one(query)

    @staticmethod
    def delete_one(collection, query):
        return Database.DATABASE[collection].delete_one(query)
