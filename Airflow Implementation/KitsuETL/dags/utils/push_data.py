

from utils import Database
import json
import logging


class ETL:
    def __init__(self):
        Database.initialize()

    def etlpipelineanime(self):
        with open('anime_sources.json','r') as fp:
            data = json.load(fp)
            fp.close()
        records = [x for j in data for x in j]
        Database.insert_many('nimesources', records)