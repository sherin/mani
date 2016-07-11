
import logging
import redis_lock
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

class Job:
    def __init__(self, name, period, func, redis):
        self.name = name
        self.period = period
        self.func = func
        self.redis = redis
        self.last_ran = datetime.min

    def run(self, now):

        lock = redis_lock.Lock(self.redis, self.name, expire=60)
        if lock.acquire(blocking=False):
            log.info("running job %s", self.name)

            self.last_ran = now
            self.func()

    def ready_to_run(self, now):
        run_at = self.last_ran + timedelta(minutes=self.period)

        if run_at > now:
            return False

        return True
