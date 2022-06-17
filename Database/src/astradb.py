from astrapy.collections import create_client, AstraCollection
import uuid
import os


class Database(object):
    ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
    ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
    ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
    ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
    SESSION = None
    
    @staticmethod
    def initialize():
        astra_client = create_client(astra_database_id=Database.ASTRA_DB_ID,
                                    astra_database_region=Database.ASTRA_DB_REGION,
                                    astra_application_token=Database.ASTRA_DB_APPLICATION_TOKEN)
        Database.SESSION = astra_client.namespace(Database.ASTRA_DB_KEYSPACE)
        
        
    @staticmethod
    def insert(jsonobj:dict,collection:str):
        operation = Database.SESSION.collection(collection)
        cliff_uuid = str(uuid.uuid4())
        operation.create(path=cliff_uuid, document=jsonobj)