import os
import logging
import sys
import traceback
LOGGER = logging.getLogger(__name__)
from utilty.db_connect import *


def extract_data():
   
   LOGGER.info("Query excution Started", extra={"end_point":"extract_data query execution started"})
   
   try:
       
    query = 'select CONCAT(first_name," ",last_name as name) as student_name, student_email, primary_email,' \
                'home_phone, cell_phone from qb.DemoData'
    status , recordset = execute_db_query(query)
    LOGGER.info("Query excution end", extra={"status":status})
    
    if status:
        for record in recordset:
            print('row = %r' % (record,))
    else:
       LOGGER.error(msg=f"DB Error")

   except Exception as e:
      LOGGER.error(
                msg=f"DB Error",
                extra= traceback.format_exception(*sys.exc_info())
            )

def main():
    init_logs()
    extract_data()

if __name__ == '__main__':
    main()