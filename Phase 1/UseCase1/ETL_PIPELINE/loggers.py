import logging

logger = logging

# logging basic config method and saving log files
logger.basicConfig(filename=r'pipeline.log', level=logging.INFO,
                    format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger.basicConfig(filename=r'pipeline.log', level=logging.ERROR,
format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')