import os
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

class Import_File:

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

    def read_file(self):
        '''
        Description:
            This function is used to read json and creates a dataframe.
        Parameter:
            it takes self as parameter.
        '''
        try:
            
            self.data = pd.read_csv(r'C:\Users\Ranjith.gc\Training(Ranjith)\Phase 1\UseCase1\ETL_PIPELINE\sales_data_sample.csv')    
            logger.info(self.data)
        
        except Exception as e:
            logger.error(e)

    def create_table(self):
        '''
        Description:
            This function is used to create table.
        Parameter:
            it takes self as parameter.
        '''
        try:

            self.db_cursor.execute('''CREATE TABLE sales_data_new1
                (ORDERNUMBER INTEGER(15),
                QUANTITYORDERED INTEGER(15),
                PRICEEACH DOUBLE,
                ORDERLINENUMBER INTEGER(15),
                SALES DOUBLE,
                ORDERDATE VARCHAR(30),
                STATUS VARCHAR(30),
                QTR_ID INTEGER(10),
                MONTH_ID INTEGER(10),
                YEAR_ID INTEGER(10),
                PRODUCTLINE VARCHAR(30)
                )''')
            logger.info("table created"),

        except Exception as e:
            logger.error(e)

    def insert(self):
        
        try:
            
            for i,row in self.data.iterrows():
                sql = "INSERT INTO sales_data_new1 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.db_cursor.execute(sql, tuple(row))
                logger.info("records inserted")
                self.db_connection.commit()
        
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    crud = Import_File()
    crud.make_connection()
    crud.read_file()
    crud.create_table()
    crud.insert()