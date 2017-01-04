
import util

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

from scheduler import Scheduler

class Mani:

    def __init__(self, redis_url, config = {}):
        self.redis = util.redis_conn(redis_url)
        self.scheduler = Scheduler(redis=self.redis, config=config)

    def every(self, seconds=0, minutes=0, hours=0, days=0, weeks=0, at=None):

        def inner(job_func):
            period = seconds + minutes * 60
            period += hours * 60 * 60
            period += days * 24 * 60 * 60
            period += weeks * 7 * 24 * 60 * 60
            self.scheduler.add_job(period, at, job_func)

        return inner

    def start(self):
        self.scheduler.start()

    def stop(self):
        self.scheduler.stop()

