import pyodbc
from loggers import logger
import configparser 
config = configparser.ConfigParser()
config.read(r'C:\Users\Ranjith.gc\Training(Ranjith)\Phase 1\Day 3\basic_queries_using_pyodbc\config.ini')

UserId=config.get('account','USER')
password=config.get('account','PASSWORD')
host=config.get('account','HOST')
database=config.get('account','DATABASE')
driver=config.get('account','DRIVER')

class CrudOperation:

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

    def print_connection(self):
        '''
        Description:
            this function prints the connection object.
        Parameter:
            it takes self as parameter.
        '''
        
        try:
          
            self.db_connection = pyodbc.connect(f'DRIVER={driver};User ID={UserId};Password={password};Server={host};Database={database}')

            self.db_cursor = self.db_connection.cursor()

            print(self.db_connection)
        
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    crud = CrudOperation()
    crud.print_connection()