import pytz
import unittest
from datetime import datetime, timedelta
from freezegun import freeze_time

from mani.job import Job
from mani import util

redis = util.redis_conn("redis://localhost:6379/")

jobs_ran = []
def run_job():
    jobs_ran.append(1)

class TestJob(unittest.TestCase):

    config = {
      "timeout": 60,
      "heartbeat_key": "mani:heartbeat",
      "timezone": pytz.timezone('US/Pacific')
    }

    def test_init(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.utcnow().replace(tzinfo=pytz.utc)

        self.assertIsNotNone(job)
        self.assertIs(job.is_running(), False)
        self.assertEqual(job.last_ran(now), now - timedelta(seconds=1))

    def test_should_run(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.utcnow().replace(tzinfo=pytz.utc)

        self.assertIs(job.ready_to_run(now), True)

    def test_run(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.utcnow().replace(tzinfo=pytz.utc)

        self.assertEqual(len(jobs_ran), 0)
        self.assertIs(job.ready_to_run(now), True)
        job.run(now)
        self.assertIs(job.ready_to_run(now), False)
        self.assertEqual(len(jobs_ran), 1)

    def tearDown(self):
        global jobs_ran
        jobs_ran = []
        redis.delete("mani:job:test")


class TestJobWithDSTIn(TestJob):

    def test_run_dst_in(self):
        # PT - March 11, 1:59 am (+8 UTC). At 2 am, DST kicks in moving time 1 hr forward (+7 UTC)
        dt = datetime(2018, 3, 11, 9, 59, tzinfo=pytz.utc)
        job = Job("test", 1, None, run_job, redis, self.config)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            self.assertEqual(len(jobs_ran), 0)
            self.assertIs(job.ready_to_run(now), True)
            job.run(now)
            self.assertIs(job.ready_to_run(now), False)
            self.assertEqual(len(jobs_ran), 1)

            # move time forward
            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            self.assertIs(job.ready_to_run(now), True)
            job.run(now)
            self.assertIs(job.ready_to_run(now), False)
            self.assertEqual(len(jobs_ran), 2)

class TestJobWithDSTOut(TestJob):

    def test_run_dst_out(self):
        # PT - Nov 4, 1:59 am (+7 UTC). At 2 am, DST kicks in moving time 1 hr behind (+8 UTC)
        dt = datetime(2018, 11, 4, 8, 59, tzinfo=pytz.utc)
        job = Job("test", 1, None, run_job, redis, self.config)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            self.assertEqual(len(jobs_ran), 0)
            self.assertIs(job.ready_to_run(now), True)
            job.run(now)
            self.assertIs(job.ready_to_run(now), False)
            self.assertEqual(len(jobs_ran), 1)

            # move time forward
            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            self.assertIs(job.ready_to_run(now), True)
            job.run(now)
            self.assertIs(job.ready_to_run(now), False)
            self.assertEqual(len(jobs_ran), 2)

if __name__ == '__main__':
    unittest.main()


