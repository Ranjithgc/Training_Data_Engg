from loggers import logger

def add(a, b):
    c = a + b
    logger.info("sum:")
    logger.info(c)

def sub(a, b):
    c = a - b
    logger.info("subtraction:")
    logger.info(c)

def mul(a, b):
    c = a * b
    logger.info("product:")
    logger.info(c)

def div(a, b):
    c = a / b
    logger.info("divisor:")
    logger.info(c)

def square(x):
    y=x*x
    logger.info("square of number is:")
    logger.info(y)

add(3, 5)
sub(8,6)
mul(12, 5)
div(10,2)
square(10)