import snowflake.connector
from loggers import logger
from retry import retry
import config

class Snowflake_python:

    def __init__(self) -> None:
        pass 

    @retry(3)
    def make_connection(self):
    
        try:
    
            self.conn=snowflake.connector.connect(
                user=config.user,
                password=config.password,
                account=config.account,
                warehouse=config.warehouse,
                database=config.database,
                schema=config.schema
            )
    
            #create cursor
            self.curs=self.conn.cursor()

            #execute SQL statement
            self.curs.execute("select current_version()")
    
            #fetch result
            logger.info(self.curs.fetchone()[0])

        except Exception as e:
            logger.error(e)

    def create_warehouse_database_schema(self):
        try:
    
            self.curs.execute("CREATE WAREHOUSE IF NOT EXISTS Ranjith")
            logger.info("warehouse created")
            self.curs.execute("USE WAREHOUSE Ranjith")
            
            self.curs.execute("CREATE DATABASE IF NOT EXISTS ARUN")
            logger.info("database created")
            self.curs.execute("USE DATABASE ARUN")
            
            self.curs.execute("CREATE SCHEMA IF NOT EXISTS ANUP")
            logger.info("schema created")
            self.curs.execute("USE SCHEMA ARUN.ANUP")

        except Exception as e:
            logger.error(e)

    def create_table(self):

        try:
            
            self.curs.execute("CREATE OR REPLACE TABLE ARUN.ANUP.student(id integer, name string, course string)" )
            logger.info("Table created")

        except Exception as e:
            logger.error(e)
    
    def insert(self):

        try:

            #self.curs.execute("INSERT INTO ARUN.ANUP.student(id,name,course) VALUES(%s,%s,%s)", ('101','Ranjith','MCA'))
            #logger.info("record inserted")
            sql = "INSERT INTO ARUN.ANUP.student(id, name,course) VALUES(%s, %s, %s)"
            val = [(2, 'Arun','Btech'),
                    (3, 'Lohith', 'BCA'),
                    (4, 'Vinay', 'Bcom'),
                    (5, 'Vinay', 'MCA')]

            self.curs.executemany(sql, val)

            self.conn.commit()
        
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
            self.curs.execute("SELECT *FROM ARUN.ANUP.student")

            result = self.curs.fetchall()

            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    snow = Snowflake_python()
    snow.make_connection()
    #snow.create_warehouse_database_schema()
    #snow.create_table()
    #snow.insert()
    snow.select()