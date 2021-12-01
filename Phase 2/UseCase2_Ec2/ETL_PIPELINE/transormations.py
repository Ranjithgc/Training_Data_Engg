import pandas as pd
from loggers import logger

class Transformations:

    def __init__(self):
        pass

    def transform(self, result_df):
        
        try:
            sales_df = result_df
            sales_df['QTR_ID'] = pd.DatetimeIndex(sales_df['ORDERDATE']).quarter
            sales_df['YEAR_ID'] = pd.DatetimeIndex(sales_df['ORDERDATE']).year
            sales_df['MONTH_ID'] = pd.DatetimeIndex(sales_df['ORDERDATE']).month
            sales_df['ORDERDATE'] = sales_df['ORDERDATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
            sales_df.rename(columns = {'STATUS' : 'STATUS1'}, inplace = True)
            logger.info(sales_df)
            return sales_df
        
        except Exception as e:
            logger.error(e)