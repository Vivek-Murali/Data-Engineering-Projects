from .mongodb import Database as MongoDatabase
from .astradb import Database as AstraDatabase
from .cassandradb import Database as CassandraDatabase

CassandraDatabase.initialize()
MongoDatabase.initialize()
AstraDatabase.initialize()