import requests
import os
import logging
import sys
import traceback
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
load_dotenv()
QB_Base_path = os.getenv('QB_Base_path')

def insertOrUpdateRecordQD(headers,data,QB_appId,table_id):
    processed_student_data = list()
    no_processed_student_data = list()
    for item in data:
        try:
            student_name = item.get('student_name','')
            student_email = item.get('student_email','')
            primary_email = item.get('primary_email','')
            home_phone = item.get('home_phone','')
            cell_phone = item.get('cell_phone','')
            body = {
                    "to": table_id,
                    "data" : [{
                            1:{'value': student_name},
                            2:{'value': student_email},
                            3:{'value': primary_email},
                            4:{'value': home_phone},
                            5:{'value': cell_phone} 
                        },],
                    "fieldsToReturn" : []
            }
            response= requests.post(f"{QB_Base_path}/records",headers,json = body)
            if response.status_code == 200:
                metadata = response.data.get('metadata',{})
                if metadata.get('totalNumberOfRecordsProcessed','') == 1:                                    
                    LOGGER.info("records", extra={"status":'success','student_name':student_name})
                    processed_student_data.append(item)
                else:                                   
                    LOGGER.warning("records", extra={"status":"Failed",'student_name':student_name})
                    no_processed_student_data.append(item)
            else:
                LOGGER.warning("records", extra={'status_code':response.status_code,"status":"Failed",'student_name':student_name})
                no_processed_student_data.append(item)
        except Exception as e:
            no_processed_student_data.append(item)
            LOGGER.error(
                        msg=f"Http Error",
                        extra= traceback.format_exception(*sys.exc_info())
                    )
    return len(processed_student_data),len(no_processed_student_data)

            

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
    return response.data , response.status_code
    
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