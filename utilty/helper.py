import requests
import os
from dotenv import load_dotenv

load_dotenv()
QB_Base_path = os.getenv('QB_Base_path')

def insertOrUpdateRecordQD(headers,data,QB_appId):
    body = {
            "to": "bck7gp3q2",
            "data" : data,
            "fieldsToReturn" : []
    }
    response= requests.post(f"{QB_Base_path}/records",headers,json = body)

def createTableQB(headers,data,QB_appId):
    body = {
    "name": "My table",
    "description": "my first table",
    "singleRecordName": "record",
    "pluralRecordName": "records"
    }   
    params = {
  	'appId': '{QB_appId}'
    }
    response= requests.post(f"{QB_Base_path}/tables",headers,json=body)
    
def createQBApp(headers):
    body={
    "name": "My App",
    "description": "My first app",
    "assignToken": True,
    "securityProperties": {
        "allowClone": False,
        "allowExport": False,
        "enableAppTokens": False,
        "hideFromPublic": False,
        "mustBeRealmApproved": True,
        "useIPFilter": True
    },
    "variables": [
        {
        "name": "Variable1",
        "value": "Value1"
        }
    ]
    }
    response= requests.post(f"{QB_Base_path}/apps",headers,json=body)
    return response.data.get('name')