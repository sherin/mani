
import logging
import redis_lock
from datetime import datetime, timedelta

import util

log = logging.getLogger(__name__)

class Job:
    def __init__(self, name, period, func, redis):
        self.name = name
        self.period = period
        self.func = func
        self.redis = redis
        self.last_ran = self.get_last_ran()

    def run(self, now):

        lock = redis_lock.Lock(self.redis, self.name, expire=60)
        if lock.acquire(blocking=False):
            log.info("running job %s", self.name)

            self.set_last_ran(now)
            self.func()

    def ready_to_run(self, now):
        run_at = self.last_ran + timedelta(minutes=self.period)

        if run_at > now:
            return False

        return True

    def last_ran_key(self):
        return "zhong:job:%s" % self.name

    def set_last_ran(self, now):
        self.redis.set(self.last_ran_key(), util.to_timestamp(now))

    def get_last_ran(self):
        last_ran = self.redis.get(self.last_ran_key())
        if last_ran:
            last_ran = util.to_datetime(last_ran)
        else:
            last_ran = datetime.min
        return last_ran
