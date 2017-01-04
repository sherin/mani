
import gc
import logging
import os
import signal
import socket
import time
from datetime import datetime

from job import Job

log = logging.getLogger(__name__)

class Scheduler:

    DEFAULT_CONFIG = {
      "timeout": 60,
      "heartbeat_key": "mani:heartbeat",
    }

    TRAPPED_SIGNALS = (
        signal.SIGINT,
        signal.SIGTERM,
        signal.SIGQUIT
    )

    def __init__(self, redis, config = {}):
        self.jobs = {}
        self.redis = redis
        self.host = socket.gethostname()
        self.pid = os.getpid()

        self.running = False
        self.stopped = False

        self.config = self.DEFAULT_CONFIG.copy()
        self.config.update(config)

    def add_job(self, period, at, job_func):
        name = job_func.__name__
        if name in self.jobs:
            raise "duplicate job %s" % name

        job = Job(name, period, at, job_func, self.redis, self.config)
        self.jobs[name] = job

    def start(self):
        self.running = True
        self.trap_signals()

        while True:
            if self.stopped: break

            now = self.now()

            jobs = self.jobs_to_run(now)
            for job in jobs:
                job.run(now)

            self.heartbeat(now)

            if self.stopped: break

            self.sleep_until_next_second()

        log.info("stopped")

    def jobs_to_run(self, now):
        return filter(lambda j: j.ready_to_run(now), self.jobs.values())

    def heartbeat(self, now):
        self.redis.hset(self.config["heartbeat_key"], self.heartbeat_field(), now)

    def heartbeat_field(self):
        return "%s##%s" % (self.host, self.pid)

    def now(self):
        return datetime.now()

    def trap_signals(self):
        try:
            for sig in self.TRAPPED_SIGNALS:
                signal.signal(sig, self.stop)
        except ValueError: # for tests to pass (since it runs on a thread)
            log.warning("could not add handlers for trapping signals")

    def stop(self, _signal=None, _frame=None):
        self.stopped = True

    def sleep_until_next_second(self):
        # process gets hot otherwise
        gc.collect()

        now = datetime.now()
        sleeptime = 1.0 - (now.microsecond / 1000000.0)
        time.sleep(sleeptime)

