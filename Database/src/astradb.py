from astrapy.collections import create_client, AstraCollection
import requests
import uuid
import os
import json


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
    def insertviasdk(jsonobj:dict,collection:str):
        operation = Database.SESSION.collection(collection)
        cliff_uuid = str(uuid.uuid4())
        operation.create(path=cliff_uuid, document=jsonobj)
        
    @staticmethod
    def createtable(properties:dict):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v1/keyspaces/{}/tables".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION,
                                                                                                    Database.ASTRA_DB_KEYSPACE)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                } 
        PAYLOAD = json.dumps(properties)
        request = requests.post(URL, data=PAYLOAD, headers=HEADER)
        return request.json()
    
    @staticmethod
    def createnamespace(collection:str):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/schemas/namespaces".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        PAYLOAD = json.dumps({"name": collection})
        request = requests.post(URL, data=PAYLOAD, headers=HEADER)
        return request.status_code
    
    @staticmethod
    def checknamespace(collection:str):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/schemas/namespaces/{}".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION,
                                                                                                    collection)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        request = requests.get(URL,headers=HEADER)
        return request.status_code
    
    @staticmethod
    def getallnamespace():
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/schemas/namespaces".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        request = requests.get(URL,headers=HEADER)
        return request.json()
    
    @staticmethod
    def createcollection(collection:str):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/namespaces/{}/collections".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION,
                                                                                                    Database.ASTRA_DB_KEYSPACE)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        PAYLOAD = json.dumps({"name": collection})
        request = requests.post(URL, data=PAYLOAD, headers=HEADER)
        return request.status_code
    
    @staticmethod
    def insert(jsonobj,collection:str):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/namespaces/{}/collections/{}".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION,
                                                                                                    Database.ASTRA_DB_KEYSPACE,
                                                                                                    collection)
        HEADER={"content-type": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        cliff_uuid = str(uuid.uuid4())
        jsonobj['uuid'] = cliff_uuid
        print(jsonobj)
        
        PAYLOAD = json.dumps(jsonobj)
        request = requests.post(URL, data=PAYLOAD, headers=HEADER)
        return request.json()
    
    @staticmethod
    def insertrow(jsonobj:dict,collection:str):
        URL = "https://{}-{}.apps.astra.datastax.com/api/rest/v2/keyspaces/{}/{}".format(Database.ASTRA_DB_ID,
                                                                                                    Database.ASTRA_DB_REGION,
                                                                                                    Database.ASTRA_DB_KEYSPACE,
                                                                                                    collection)
        HEADER={"content-type": "application/json",
                "Accept": "application/json",
                "x-cassandra-token": Database.ASTRA_DB_APPLICATION_TOKEN
                }
        PAYLOAD = json.dumps(jsonobj)
        request = requests.post(URL, data=PAYLOAD, headers=HEADER)
        return request.json()
    
    