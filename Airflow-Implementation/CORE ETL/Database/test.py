from src import (CassandraDatabase, AstraDatabase)
import json
import uuid


with open("final_copy.json",'r') as file:
    data = json.load(file)
    file.close()

#print(data[1])

#print(AstraDatabase.createnamespace("research_contents"))
#AstraDatabase.checknamespace("research_contents")
#print(AstraDatabase.createcollection('core_contents'))

schema = [ {"name":"id","typeDefinition":"uuid","static":False},
        {"name":"doi","typeDefinition":"text","static":False}, 
        {"name":"coreId","typeDefinition":"text","static":False}, 
        {"name":"oai","typeDefinition":"text","static":False},
        {"name":"identifier","typeDefinition":"frozen list","static":False},
        {"name":"title","typeDefinition":"text","static":False},
        {"name":"contributors","typeDefinition":"frozen list","static":False},
        {"name":"abstract","typeDefinition":"text","static":False},
        {"name":"datePublished","typeDefinition":"text","static":False},
        {"name":"downloadUrl","typeDefinition":"text","static":False},
        {"name":"fullTextIdentifier","typeDefinition":"text","static":False},
        {"name":"pdfHashValue","typeDefinition":"text","static":False},
        {"name":"publisher","typeDefinition":"text","static":False},
        {"name":"year","typeDefinition":"text","static":False},
        {"name":"fullText","typeDefinition":"text","static":False},
        {"name":"urls","typeDefinition":"frozen list","static":False},
        {"name":"relations","typeDefinition":"frozen list","static":False},
        {"name":"authors","typeDefinition":"frozen list","static":False},
        {"name":"topics","typeDefinition":"frozen list","static":False},
        {"name":"subjects","typeDefinition":"frozen list","static":False},
        {"name":"issn","typeDefinition":"text","static":False},
        {"name":"magId","typeDefinition":"double", "static":False},
        {"name":"created","typeDefinition":"timestamp","static":False}]



CassandraDatabase.check_connection()
CassandraDatabase.fetchall('core_contents')
#print(AstraDatabase.insert(data[0],'core_contents'))