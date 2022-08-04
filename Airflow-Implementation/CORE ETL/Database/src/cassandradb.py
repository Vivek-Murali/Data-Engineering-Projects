from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
import uuid
from dotenv import load_dotenv
load_dotenv()


class Database(object):
    CLOUD_CONFIG= {
                    'secure_connect_bundle':os.getenv('SECURE_CONNECTION')
                    }
    CLIENT_ID = os.getenv('CASSANDRA_CLIENT_ID')
    CLIENT_PASSWORD = os.getenv('CASSANDRA_CLIENT_PASSWORD')
    KEYSPACE = os.getenv('KEYSPACE')
    database = os.getenv("DB_NAME")
    SESSION = None

    @staticmethod
    def initialize():
        auth_provider = PlainTextAuthProvider(Database.CLIENT_ID,Database.CLIENT_PASSWORD)
        cluster = Cluster(cloud=Database.CLOUD_CONFIG, auth_provider=auth_provider)
        Database.SESSION = cluster.connect()
        Database.SESSION.set_keyspace(os.getenv('KEYSPACE'))
        
    @staticmethod
    def check_connection():
        row = Database.SESSION.execute("select release_version from system.local").one()
        if row:
            print(row[0])
        else:
            print("An error occurred.")
            
    @staticmethod
    def insert(collection, data):
        pass
    
    @staticmethod
    def fetchall(collection:str,limit:int=100):
        row = Database.SESSION.execute("select * from "+collection+" limit 100")
        if row:
            print(row.one())
            return row
        else:
            print("An error occurred.")
            
    @staticmethod
    def change_column_type(table:str,column:str,datatype:str):
        row = Database.SESSION.execute("ALTER TABLE "+Database.KEYSPACE+"."+table+" ALTER "+ column +" TYPE "+ datatype+" ;")
        if row:
            print(row.one())
            return row
        else:
            print("An error occurred.")