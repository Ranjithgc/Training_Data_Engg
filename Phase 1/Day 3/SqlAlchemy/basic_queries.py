import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from loggers import logger
import secrets

db.__version__

class SqlAlchemy:
    def __init__(self) -> None:
        pass
    
    def make_connection(self):

        try:
            
            self.engine = db.create_engine("mysql+mysqlconnector://secrtes.user:secrets.password@localhost/secrets.dbase")

        except Exception as e:
            logger.error(e)

    def connect_databse(self):
        try:
            self.connection = self.engine.connect()
            self.metadata = db.MetaData()
            self.emp = db.Table('employee', self.metadata, autoload=True, autoload_with=self.engine)
        except Exception as e:
            logger.error(e)

    def show_columns(self):
        try:
            print(self.emp,[].columns.keys())
        except Exception as e:
            logger.error(e)
    
    def repr_table(self):
        try:
            print(repr(self.metadata.tables['emp']))
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    sql1 = SqlAlchemy()
    sql1.make_connection()
    sql1.connect_databse()
    sql1.show_columns()
    sql1.repr_table()