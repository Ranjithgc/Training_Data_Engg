import snowflake.connector
from loggers import logger
import configparser 
from retry import retry
from snowflake.connector.pandas_tools import write_pandas

config = configparser.ConfigParser()
config.read(r'C:\Users\Ranjith.gc\Training(Ranjith)\Phase 1\UseCase1\ETL_PIPELINE\config.ini')

UserId=config.get('snowflake','user')
password=config.get('snowflake','password')
account=config.get('snowflake','account')
warehouse=config.get('snowflake','warehouse')
database=config.get('snowflake','DATABASE')
schema=config.get('snowflake','schema')

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
                schema=schema
            )
            self.db_cursor = self.db_connection.cursor()

            logger.info(self.db_connection)
        
        except Exception as e:
            logger.error(e)

    def load(self,sales):
        
        try:

            # for i,row in sales.iterrows():
            #     sql = f"INSERT INTO ARUN.ANUP.s_data VALUES (%s,%s,%s,%s,%s,%s)"
            #     self.db_cursor.execute(sql, tuple(row))
            #     logger.info("records inserted")
            #     self.db_connection.commit()

            success, nchunks, nrows, _ = write_pandas(self.db_connection, sales, 'SALES_DATA')
            logger.info("success : "+str(success))
            logger.info("nchunks : "+str(nchunks))
            logger.info("number of rows : "+str(nrows))

        except Exception as e:
            logger.error(e)

    