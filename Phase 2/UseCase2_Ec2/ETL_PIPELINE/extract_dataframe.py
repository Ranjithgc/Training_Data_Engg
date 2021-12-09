import datetime
from loggers import logger
import pandas as pd
from datetime import timedelta,datetime

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
    
    def create_dataframe(self):
        '''
        Description:
            this function prints the connection object.
        Parameter:
            it takes self as parameter.
        '''
        
        try:

            self.sales_data = pd.read_csv(r'/home/ubuntu/UseCase1/ETL_PIPELINE/sales_data_sample.csv')

        except Exception as e:
            logger.error(e)
    
    def extract(self):

        try:
            
            self.sales_data['ORDERDATE'] = pd.to_datetime(self.sales_data['ORDERDATE'])
            now = datetime.now()
            print(now)
            subtracted_date = now - timedelta(days=1)
            add_hour = subtracted_date + timedelta(hours=1)
            extract_data = self.sales_data[(self.sales_data['ORDERDATE']>=pd.to_datetime(subtracted_date)) & (self.sales_data['ORDERDATE']<=pd.to_datetime(add_hour))]          
            extract_data.loc[:,'ORDERDATE']=extract_data['ORDERDATE'].astype(object)
            logger.info(extract_data)
            return extract_data

        except Exception as e:
            logger.error(e)

