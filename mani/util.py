
import pytz
import redis
import time

from datetime import datetime, timedelta
try:
    # Python 3
    from urllib.parse import urlparse
except ImportError:
    # Python 2
    from urlparse import urlparse

def redis_conn(redis_url):
    info = urlparse(redis_url)
    host = info.hostname
    port = info.port
    password = info.password

    return redis.StrictRedis(
        host=host,
        port=port,
        password=password
    )

def to_datetime(ts):
    return datetime.utcfromtimestamp(float(ts)).replace(tzinfo=pytz.utc)

def to_timestamp(dt):
    return time.mktime(dt.timetuple())

def to_timestamp_utc(dt):
    utc_naive  = dt.replace(tzinfo=None) - dt.utcoffset()
    timestamp = (utc_naive - datetime(1970, 1, 1)).total_seconds()
    return timestamp

def next_weekday(wday, dt):
    if dt.weekday() == wday:
        return dt
    while (dt.weekday() != wday):
        dt = dt + timedelta(days = 1)
    return dt
