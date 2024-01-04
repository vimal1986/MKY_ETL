# ETL job
As a part of integrating  the HS student data from Kourier ODS to Quickbase we have to first establish a connection to the MS SQL server.According to the QuickBase project requirements a SQL view will be created in the Kourier ODS.The project helps to fetch the HS student data from the qb_view. This data can be then modified and can be integrated to tyhe Quickbase.

System Requirement

Need python version >= 3.8

Python Modules used in this project

1. pyodbc
    - Python module that makes accessing ODBC databases(MSSQL)
2. python-dotenv
    - Python-dotenv reads key-value pairs from a .env file and can set them as environment variables    
3. velebit-useful-logs
    - A simple function for Python logging setup. Using this allows for the consistent log format across multiple programs, which is very convenient when using microservice architecture

Steps to set-up the project

For Mac 

brew install unixodbc

pip uninstall pyodbc

pip install --no-binary :all: pyodbc

For Windows follow the below documentation link

    https://learn.microsoft.com/en-us/sql/connect/odbc/windows/microsoft-odbc-driver-for-sql-server-on-windows?view=sql-server-2017

1.pip install -r requirements.txt 

2. To run file python app.py