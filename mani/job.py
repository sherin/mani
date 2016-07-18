
import logging
import redis_lock
from datetime import datetime, timedelta

import util

log = logging.getLogger(__name__)

class Job:
    def __init__(self, name, period, func, redis, config):
        self.name = name
        self.period = period
        self.func = func
        self.redis = redis
        self.running = False
        self.config = config

    def run(self, now):

        lock = redis_lock.Lock(self.redis, self.name, expire=self.config["timeout"])
        if lock.acquire(blocking=False):
            try:
                if not self.ready_to_run(now):
                    return
                log.info("running job %s", self.name)

                self.running = True
                self.set_last_ran(now)

                try:
                    self.func()
                except:
                    log.exception("%s job failed to run!" % self.name)
            finally:
                lock.release()
        else:
            log.info("could not acquire lock for job %s", self.name)

        self.running = False

    def ready_to_run(self, now):
        last_ran = self.last_ran()
        run_at = last_ran + timedelta(seconds=self.period)

        if run_at > now or last_ran > now:
            return False

        log.debug("%s run_at: %s, last_ran: %s, now: %s", self.name, run_at, last_ran, now)
        return True

    def last_ran_key(self):
        return "mani:job:%s" % self.name

    def set_last_ran(self, now):
        self.redis.set(self.last_ran_key(), util.to_timestamp(now))

    def last_ran(self):
        last_ran = self.redis.get(self.last_ran_key())
        if last_ran:
            last_ran = util.to_datetime(last_ran)
        else:
            last_ran = datetime.min
        return last_ran

    def is_running(self):
        return self.running
