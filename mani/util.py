
import redis
import time
import urlparse
from datetime import datetime

def redis_conn(redis_url):
    info = urlparse.urlparse(redis_url)
    host = info.hostname
    port = info.port
    password = info.password

    return redis.StrictRedis(
        host=host,
        port=port,
        password=password
    )

def to_datetime(ts):
    return datetime.fromtimestamp(float(ts))

def to_timestamp(dt):
    return time.mktime(dt.timetuple())
