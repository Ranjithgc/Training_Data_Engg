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
            df.to_csv(f"C:\Users\Ranjith.gc\Training(Ranjith)\Phase 2\UseCase2_Ec2\sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv", header=False, index=False)
            logger.info("csv file created and uploaded dataframe to csv file")

        except Exception as e:

            logger.error(e)

    def upload_json_s3(self):
        
        try:
            
            self.s3.Bucket('etlpipeline').upload_file(Filename=f"sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv", Key=f"sales-data/sales_{datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')}.csv")

            logger.info("file uploaded")

        except Exception as e:

            logger.error(e)

