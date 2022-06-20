import pandas as pd
import numpy as np
import json
import logging as logger
from os import listdir
from os.path import isfile, join
import sys
from  pathlib import Path 
import tarfile
import os
import shutil
import random
import aiofiles
import asyncio
from concurrent.futures import ThreadPoolExecutor
from Database.src import (AstraDatabase, CassandraDatabase, UtilityFunctions)


COUNTER = 0

logger.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s', level=logger.INFO, datefmt="%d-%b-%y %H:%M:%S")

def extract_json_data(filename:str):
    onlyfiles = [f for f in listdir(filename) if isfile(join(filename, f))]
    logger.info("Number of Files to Process %s"%len(onlyfiles))
    onlyfiles = random.choices(onlyfiles,k=30)
    [make_json_data(os.path.join("Data",f),number) for number,f in enumerate(onlyfiles) if f not in listdir(os.path.join("json","Data"))]
    logger.info("Extracting Process Done")


def make_json_data(filename:str,number:int):
    try:
        tfile = tarfile.open(filename,"r:xz")
        logger.info("File No:%f"%number)
        tfile.extractall(path=os.path.join("json",filename))
        tfile.close()
    except:
        pass
    
def push_data_into_one():
    Destination:str = os.path.join("json","Final")
    [shutil.move(os.path.join(path, name), os.path.join(Destination,name)) for path, subdirs, files in os.walk(os.path.join("json","Data")) for name in files if name.endswith('.json')]
    logger.info("Files Movement Completed")
    
    
def read_file_from_json(filename:str):
    logger.info("File Read from JSON:%s"%filename)
    with open(filename, mode='r') as f:
        jsonfile = json.load(f)
        f.close()
    result = UtilityFunctions.makecoreresponse(jsonfile)
    response = AstraDatabase.insertrow(result,'datasource_core')
    logger.info("File Response:%s"%response)
    
def create_table(properties:dict):
    return AstraDatabase.createtable(properties)
    
async def make_single_file(number:int=1000000)->list:
    files = [os.path.join('json/Final',f) for f in listdir(os.path.join("json","Final"))]
    logger.info("Number Of Files Collected Successfully %s"%len(files))
    files = files[:number]
    with ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(read_file_from_json,files)
    
    
if __name__ == '__main__':
    #extract_json_data("Data")
    #push_data_into_one()
    #asyncio.run(make_single_file())
    CassandraDatabase.check_connection()
    """ print(AstraDatabase.createudt({
                    "name": "journals",
                    "ifNotExists": True,
                    "fields": [
                            {
                            "name": "identifiers",
                            "typeDefinition": "list<text>"
                            },
                            {
                                "name": "title",
                                "typeDefinition": "text"
                            }]}))
    with open("properties.json",'r') as fp:
        properties = json.load(fp)
        fp.close()
    print(AstraDatabase.deletetable('datasource_core'))
    print(create_table(properties)) """
    asyncio.run(make_single_file())
    #rows = CassandraDatabase.fetchall('datasource_core')
