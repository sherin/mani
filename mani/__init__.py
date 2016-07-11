
import util

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

from scheduler import Scheduler

class Mani:

    def __init__(self, redis_url, config = {}):
        self.redis = util.redis_conn(redis_url)
        self.scheduler = Scheduler(redis=self.redis, config=config)

    def every(self, minutes):

        def inner(job_func):
           self.scheduler.add_job(minutes, job_func)

        return inner

    def start(self):
        self.scheduler.start()

