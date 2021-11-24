import mysql.connector
from loggers import logger
import configparser 
from retry import retry
import pandas as pd

config = configparser.ConfigParser()
config.read(r'C:\Users\Ranjith.gc\Training(Ranjith)\Phase 1\UseCase1\ETL_PIPELINE\config.ini')

UserId=config.get('mysql','USER')
password=config.get('mysql','PASSWORD')
host=config.get('mysql','HOST')
database=config.get('mysql','DATABASE')

class Extract_dataframe:

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
          
            self.db_connection = mysql.connector.connect(
                user=UserId,
                password=password,
                host=host,
                db=database
            )
            self.db_cursor = self.db_connection.cursor()

            logger.info(self.db_connection)
        
        except Exception as e:
            logger.error(e)
    
    def extract(self):

        try:
            self.query = '''select * from sales_data_new1 
                            where str_to_date(ORDERDATE,'%m/%d/%Y %H:%i') between DATE_ADD(current_timestamp(), INTERVAL -1 DAY) 
                            and DATE_ADD(DATE_ADD(current_timestamp(), INTERVAL -1 DAY), INTERVAL +1 hour)''';

            sales_data = pd.read_sql(self.query, self.db_connection)
            logger.info(sales_data)
            return sales_data
        
        except Exception as e:
            logger.error(e)

