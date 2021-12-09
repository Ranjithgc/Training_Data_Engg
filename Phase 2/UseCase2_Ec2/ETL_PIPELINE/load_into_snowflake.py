import os
import snowflake.connector
from loggers import logger
from retry import retry
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()

UserId=os.getenv('user1')
password=os.getenv('password')
account=os.getenv('account')
warehouse=os.getenv('warehouse')
database=os.getenv('database')
schema=os.getenv('schema')
role=os.getenv('role')

class SnowFlake_load:

    def __init__(self):
        '''
        Description:
            created constructor and test mysql database connection.
        Parametter:
            it takes self as parameter.
        '''
        try:
            pass
        except Exception as e:
            logger.error(e)

    @retry(3)
    def make_connection(self):
        '''
        Description:
            this function prints the connection object.
        Parameter:
            it takes self as parameter.
        '''
        
        try:
          
            self.db_connection = snowflake.connector.connect(
                user=UserId,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role
            )
            self.db_cursor = self.db_connection.cursor()

            logger.info(self.db_connection)
        
        except Exception as e:
            logger.error(e)

    def load(self,sales):
        
        try:

            self.db_cursor.execute("use role ACCOUNTADMIN")
            self.db_cursor.execute("use warehouse RANJITH")
            self.db_cursor.execute("use database ARUN")
            self.db_cursor.execute("use schema ANUP")

            self.db_cursor.execute('''create or replace stage mystage
                                    storage_integration = s3_integrate
                                    url = 's3://etlpipeline/sales-data/'
                                    file_format = (type = CSV)''')
            
            self.db_cursor.execute("show stages")

            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute("ls @mystage")
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute('''create or replace pipe mypipe auto_ingest = true as
                                        copy into sales_data
                                        from @mystage''')
            
            self.db_cursor.execute("show pipes")

            self.db_cursor.execute("select *from sales_data")
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)

    