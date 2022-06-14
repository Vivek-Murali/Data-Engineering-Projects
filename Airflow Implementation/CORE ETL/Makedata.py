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
    
    
async def read_file_from_json(filename:str):
    async with aiofiles.open(filename, mode='r') as f:
        contents = await f.read()
        aiofiles.close(f)
    jsonfile = json.loads(contents)
    return jsonfile
    
async def make_single_file(number:int=300000)->list:
    files = [asyncio.ensure_future(read_file_from_json(os.path.join('json/Final',f))) for f in listdir(os.path.join("json","Final"))]
    logger.info("Number Of Files Collected Successfully %s"%len(files))
    moves_list = await asyncio.gather(*files)
    async with aiofiles.open('final_copy.json', mode='w') as f:
        await json.dump(moves_list,f)
    logger.info("File Saved")
    
    
if __name__ == '__main__':
    #extract_json_data("Data")
    #push_data_into_one()
    asyncio.run(make_single_file())
