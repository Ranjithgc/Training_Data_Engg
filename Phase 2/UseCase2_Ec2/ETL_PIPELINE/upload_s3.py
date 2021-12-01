from datetime import datetime
import boto3
import json
import pandas as pd
from dotenv import load_dotenv
import os
from loggers import logger

load_dotenv()

s3 = boto3.client('s3')

class Upload:

    def __init__(self):
        pass

    def create_connection(self):
        
        try:
            
            self.s3 = boto3.resource(
                service_name='s3',
                region_name='ap-south-1',
                aws_access_key_id=os.getenv("ACCESS_KEY"),
                aws_secret_access_key=os.getenv("SECRETE_KEY")
            )
        
        except Exception as e:
            logger.error(e)
    
    def convert_json(self,sales):

        try:
            
            df = pd.DataFrame(sales)
            #df.to_json('sales.json')
            df.to_csv(f"/home/ubuntu/UseCase1/sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv")
            logger.info("csv file created and uploaded dataframe to csv file")

        except Exception as e:

            logger.error(e)

    def upload_json_s3(self):
        
        try:
            #object = self.s3.Object('etlpipeline', 'sales.json')

            #result = object.put(Body=open('/home/ubuntu/UseCase1/sales.json', 'rb'))
            
            object = self.s3.Object('etlpipeline', (f"sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv"))
            
            result1 = object.put(Body=open(f"/home/ubuntu/UseCase1/sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv", 'rb'))
            logger.info("file uploaded")

        except Exception as e:

            logger.error(e)

