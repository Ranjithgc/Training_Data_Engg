from ETL_PIPELINE.extract_dataframe import *
from ETL_PIPELINE.load_into_snowflake import *
from ETL_PIPELINE.transormations import *
#from ETL_PIPELINE.notifications_slack import *
from ETL_PIPELINE.upload_s3 import *

if __name__ == "__main__":
    
    #Extract
    extra = Extract_dataframe()
    extra.create_dataframe()
    result = extra.extract()
    
    trans = Transformations()
    load = trans.transform(result)
    
    #s3
    upld = Upload()
    upld.create_connection()
    upld.convert_json(load)
    upld.upload_json_s3()

    
    #load
    snow = SnowFlake_load()
    snow.make_connection()
    snow.load(load)

    #slack_noitfy()