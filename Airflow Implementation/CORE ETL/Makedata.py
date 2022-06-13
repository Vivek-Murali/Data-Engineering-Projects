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


logger.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s', level=logger.INFO, datefmt="%d-%b-%y %H:%M:%S")

def extract_json_data(filename:str):
    onlyfiles = [f for f in listdir(filename) if isfile(join(filename, f))]
    [make_json_data(os.path.join("Data",f)) for f in onlyfiles if f not in listdir(os.path.join("json","Data"))]
    logger.info("Extracting Process Done")


def make_json_data(filename:str):
    try:
        tfile = tarfile.open(filename,"r:xz")
        tfile.extractall(path=os.path.join("json",filename))
        tfile.close()
    except:
        pass
    
def push_data_into_one():
    Destination:str = os.path.join("json","Final")
    [shutil.move(os.path.join(path, name), os.path.join(Destination,name)) for path, subdirs, files in os.walk(os.path.join("json","Data")) for name in files if name.endswith('.json')]
    logger.info("Files Movement Completed")
    
def make_single_file(number:int=150000)->list:
    files = [os.path.join('json/Final',f) for f in listdir(os.path.join("json","Final"))]
    limited_files = files[:number]
    output = []    
    for item in limited_files: 
        with open(item,'r') as fp:
            data = json.load(fp)
            fp.close() 
        output.append(data)
    logger.info("Number of Processed files %s"%len(output))
    with open("final_copy.json",'w') as fp:
        json.dump(output,fp)
        fp.close()
    logger.info("File Saved")
    


if __name__ == '__main__':
    extract_json_data("Data")
    push_data_into_one()
    make_single_file()
