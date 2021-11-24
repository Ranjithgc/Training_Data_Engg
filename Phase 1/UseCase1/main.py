from ETL_PIPELINE.extract_sql_dataframe import *
from ETL_PIPELINE.load_into_snowflake import *
from ETL_PIPELINE.transormations import *
from ETL_PIPELINE.notifications_slack import *

if __name__ == "__main__":
    
    #Extract
    extra = Extract_dataframe()
    extra.make_connection()
    result = extra.extract()
    
    trans = Transformations()
    load = trans.transform(result)
    
    #load
    snow = SnowFlake_load()
    snow.make_connection()
    snow.load(load)

    slack_noitfy()