import json
from datetime import datetime
from dateutil.parser import parse
import uuid


class UtilityFunctions(object):
    STRINGFORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    
    @staticmethod
    def fixdatetime(datetimeobj:str)->str:
        if datetimeobj:
            dt = parse(datetimeobj)
            return dt.strftime(UtilityFunctions.STRINGFORMAT)
        else:
            return None
    
    @staticmethod
    def nesteddicttostr(dictobj:dict)->str:
        if dictobj:
            return str(dictobj)
        else:
            return None
    
    @staticmethod
    def dicttomap(dictobj:dict)->list:
        results:list=[]
        if dictobj:
            for key,value in dictobj.items():
                results.append({"key":key,"value":str(value)})
            return results
        else:
            return None
    
    @staticmethod
    def makecoreresponse(jsonobj:dict)->dict:
        jsonobj['uuid'] = str(uuid.uuid4())
        jsonobj['dateUpdated'] = datetime.utcnow().strftime(UtilityFunctions.STRINGFORMAT)
        jsonobj['language'] = UtilityFunctions.dicttomap(jsonobj['language'])
        jsonobj['enrichments'] = UtilityFunctions.nesteddicttostr(jsonobj['enrichments'])
        jsonobj['datePublished'] = UtilityFunctions.fixdatetime(jsonobj['datePublished'])
        return jsonobj

