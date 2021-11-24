import os
import pyodbc
from loggers import logger
from retry import retry

class CrudOperation:

    def retries(func):
        def wrapper_retries():
            retry = 3
            for  i in range(retry):
                try:
                    func()
                    break
                except Exception as e:
                    logger.error(e)
                    logger.info('Iteration of connection' + str(i))
                    continue
            return wrapper_retries
    
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
    def print_connection(self):
        '''
        Description:
            this function prints the connection object.
        Parameter:
            it takes self as parameter.
        '''
        
        try:
            self.db_connection = pyodbc.connect('DRIVER=MYSQL ODBC 8.0 UNICODE DRIVER;User ID=root;Password=Z2M8H@pPy3;Server=localhost;Database=ARUN;String Types=Unicode')
            self.db_cursor = self.db_connection.cursor()
            logger.info(self.db_connection)
        
        except Exception as e:
            logger.error(e)
    
    def select(self):
        '''
        Description:
            This function Display the data of the table.
        Parameter:
            it takes self as parameter.
        '''
        try:

            self.db_cursor.execute("SELECT *FROM student")

            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute("SELECT *FROM employee")

            result2 = self.db_cursor.fetchall()

            for x1 in result2:
                logger.info(x1)

        except Exception as e:
            logger.error(e)
    
    def where(self):
        '''
        Description:
            This function checks where conditions and returns the row.
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("SELECT *FROM student WHERE ID = 2")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
            self.db_cursor.execute("SELECT *FROM employee WHERE ID = 3 AND SALARY > 10000")
            result1 = self.db_cursor.fetchall()
            for x1 in result1:
                logger.info(x1)

        except Exception as e:
            logger.error(e)

    def orderby(self):
        '''
        Description:
            This function sorts the row.
        Parameter:
            it takes self as parameter. 
        '''

        try:

            self.db_cursor.execute("SELECT *FROM student ORDER BY NAME")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
            self.db_cursor.execute("SELECT *FROM employee ORDER BY SALARY DESC")
            result1 = self.db_cursor.fetchall()
            for x1 in result1:
                logger.info(x1)
        
        except Exception as e:
            logger.error(e)

    def groupby(self):
        '''
        Description:
            This function arrange identical data into groups.
        Parameter:
            it takes self as parameter.
        '''

        try:
        
            self.db_cursor.execute("SELECT NAME, SUM(SALARY) FROM employee GROUP BY NAME")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
        except Exception as e:
            logger.error(e)
    
    def limit(self):
        '''
        Description: 
            This function display the rows by using limit.
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("SELECT *FROM student LIMIT 2")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
        except Exception as e:
            logger.error(e)
    
    def distinct(self):
        '''
        Description:
            This function deletes duplicate records from table.
        Parameter:
            it takes self as parameter.
        '''

        try:
        
            self.db_cursor.execute("SELECT DISTINCT NAME FROM student")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
        except Exception as e:
            logger.error(e)

    def aggregate_functions(self):
        '''
        Description:
            This function arrange identical data into groups and give condition by using having clause .
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("SELECT SUM(SALARY) FROM employee")
            result = self.db_cursor.fetchall()
            logger.info(result)
            
            self.db_cursor.execute("SELECT MAX(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)

            self.db_cursor.execute("SELECT MIN(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)

            self.db_cursor.execute("SELECT AVG(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)

            self.db_cursor.execute("SELECT COUNT(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)

        except Exception as e:
            logger.error(e)
    
    def like(self):
        '''
        Description:
            This function uses for pattern matching by using like operator.
        Parameter:
            it takes self as parameter.
        '''
        try:
            
            self.db_cursor.execute("SELECT *FROM student WHERE NAME LIKE 'V%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

            self.db_cursor.execute("SELECT *FROM student WHERE NAME LIKE '%h'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
            self.db_cursor.execute("SELECT *FROM employee WHERE NAME LIKE '_a%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
            
            self.db_cursor.execute("SELECT *FROM employee WHERE SALARY LIKE '2_%_%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

            self.db_cursor.execute("SELECT *FROM employee WHERE SALARY LIKE '_00%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    crud = CrudOperation()
    crud.print_connection()
    crud.select()
    crud.where()
    crud.orderby()
    crud.groupby()
    crud.limit()
    crud.distinct()
    crud.aggregate_functions()
    crud.like()