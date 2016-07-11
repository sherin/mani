
import urlparse
import redis

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
