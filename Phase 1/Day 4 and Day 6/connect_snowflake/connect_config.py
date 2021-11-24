import os
import snowflake.connector
from loggers import logger
import config

#create connection
def make_connection():
    try:
        conn=snowflake.connector.connect(
            user=config.user,
            password=config.password,
            account=config.account,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema
        )
    
        #create cursor
        curs=conn.cursor()

        #execute SQL statement
        curs.execute("select current_version()")
    
        #fetch result
        print(curs.fetchone()[0])
    except Exception as e:
        logger.error(e)

make_connection()
