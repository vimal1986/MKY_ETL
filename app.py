import os
import logging
import sys
import traceback
import requests
LOGGER = logging.getLogger(__name__)

from utilty.db_connect import *
from utilty.helper import insertOrUpdateRecordQD, createTableQB, createQBApp
from dotenv import load_dotenv

load_dotenv()


def extract_data():
   
   LOGGER.info("Query excution Started", extra={"end_point":"extract_data query execution started"})
   student_data = list()
   try:
       
    query = 'select CONCAT(first_name," ",last_name as name) as student_name, student_email, primary_email,' \
                'home_phone, cell_phone from qb.DemoData'
    status , recordset = execute_db_query(query)
    LOGGER.info("Query excution end", extra={"status":status})
    

    if status:
        for record in recordset:
            print('row = %r' % (record,))
            student_data.append(record)
    else:
       LOGGER.error(msg=f"DB Error")
    return student_data
   except Exception as e:
      LOGGER.error(
                msg=f"DB Error",
                extra= traceback.format_exception(*sys.exc_info())
            )
      
      
def load_data(student_data):
    QB_Authorization = os.getenv("QB_Authorization")
    QB_Realm_Hostname = os.getenv("QB_Realm_Hostname")
    QB_User_Agent = os.getenv("QB_User_Agent")
    QB_appId = os.getenv("QB_appId")
    QB_Base_path = os.getenv('QB_Base_path')
    
    headers = {
               'QB-Realm-Hostname': QB_Realm_Hostname,
               'Authorization': QB_Authorization,
               'User_Agent:': QB_User_Agent
               }
    
    response_app = requests.get(f"{QB_Base_path}/apps?{QB_appId}",headers)
    
    if response_app.status_code == 200: 
        response_tables = requests.get(f"{QB_Base_path}/tables?appId = {QB_appId}",headers)
        if response_tables == 200:
            insertOrUpdateRecordQD(headers,student_data,QB_appId)
        else:
            createTableQB(headers,{},QB_appId)
            insertOrUpdateRecordQD(headers,student_data,QB_appId)
    else:
        app_id = createQBApp(headers)
        createTableQB(headers,{},QB_appId)
        insertOrUpdateRecordQD(headers,student_data,app_id)
        
    
    
    

def main():
    init_logs()
    student_data = extract_data()
    
    load_data(student_data)

if __name__ == '__main__':
    main()