FROM python:latest
WORKDIR /.
RUN python -m pip install pandas
RUN python -m pip install python-dotenv
RUN python -m pip install pyarrow
RUN python -m pip install fastparquet
RUN python -m pip install boto3
RUN python -m pip install s3fs
RUN python -m pip install retry
RUN python -m pip install snowflake-connector-python
CMD [ "python", "./main.py" ]

