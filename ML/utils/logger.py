import logging
from functools import wraps

logging.basicConfig(level=logging.INFO)

def log_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Calling {func.__name__}")
        result = func(*args, **kwargs)
        return result
    return wrapper
