import os
import pyodbc 
import warnings
import useful.logs

from dotenv import load_dotenv

load_dotenv()

warnings.filterwarnings("ignore")


SERVER_NAME = os.getenv("SERVER_NAME")
DB_NAME = os.getenv("DB_NAME")
DRIVER = os.getenv("DRIVER")
DB_USER = os.getenv("DB_USER")
DB_VERSION = os.getenv("DB_VERSION")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

"""
Utils for Logging
"""
def init_logs():
    # default JSONFormatter fields
    JSON_FIELDS = {
        "message": "message",
        "time": "created",
        "log_level": "levelname",
        "process": "process",
        "thread": "thread",
        "traceback": "exc_text",
        "__htime": "asctime",
        'pathname': 'pathname',
        'lineno': 'lineno',
        'funcName': 'funcName'
    }

    ALWAYS_EXTRA = {
        "source": "python",
        "traceback": None,
        "trace_id": None,
        "request_id": None,
        "params": None,
        "state": None
    }

    # # Configure Logs
    useful.logs.setup(log_level="INFO", json_fields=JSON_FIELDS, always_extra=ALWAYS_EXTRA)

def execute_db_query(sql):
    status = False
    try:
        cnxn = pyodbc.connect(server=SERVER_NAME,
                database=DB_NAME,
                user=DB_USER,
                tds_version=DB_VERSION,
                password=DB_PASSWORD,
                port=DB_PORT,
                driver='FreeTDS')
        crsr = cnxn.cursor()
        recordset = crsr.execute(sql).fetchall()
        status = True
        crsr.close()
        cnxn.close()
        return status, recordset
    except pyodbc.Error as ex:
        status = False
        sqlstate = ex.args[0]
        if sqlstate == '28000':
            print("LDAP Connection failed: check password")
        return status, ""
    finally:
        crsr.close()
        cnxn.close()
    