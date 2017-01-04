
import logging
import redis_lock

import util
from run_at import RunAt

log = logging.getLogger(__name__)

class Job:
    def __init__(self, name, period, at, func, redis, config):
        self.name = name
        self.period = period
        self.at = at
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
        last_ran = self.last_ran(now)

        run_at = RunAt(self.period, self.at, now, offset=last_ran).next_at()
        log.debug("%s next run is at %s", self.name, run_at)
        if run_at > now or last_ran > now:
            return False

        log.debug("%s run_at: %s, last_ran: %s, now: %s", self.name, run_at, last_ran, now)
        return True

    def last_ran_key(self):
        return "mani:job:%s" % self.name

    def set_last_ran(self, now):
        self.redis.set(self.last_ran_key(), util.to_timestamp(now))

    def last_ran(self, now):
        last_ran = self.redis.get(self.last_ran_key())
        if last_ran:
            return util.to_datetime(last_ran)

        # new job
        last_ran = RunAt(self.period, self.at, now).last_at()
        log.debug("%s new job, last ran would have been at %s", self.name, last_ran)

        return last_ran

    def is_running(self):
        return self.running
