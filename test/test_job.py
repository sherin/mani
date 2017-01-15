import unittest
from datetime import datetime, timedelta

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
    }

    def test_init(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.now()

        self.assertIsNotNone(job)
        self.assertIs(job.is_running(), False)
        self.assertEqual(job.last_ran(now), now - timedelta(seconds=1))

    def test_should_run(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.now()

        self.assertIs(job.ready_to_run(now), True)

    def test_run(self):
        job = Job("test", 1, None, run_job, redis, self.config)
        now = datetime.now()

        self.assertEqual(len(jobs_ran), 0)
        self.assertIs(job.ready_to_run(now), True)
        job.run(now)
        self.assertIs(job.ready_to_run(now), False)
        self.assertEqual(len(jobs_ran), 1)

    def tearDown(self):
        redis.delete("mani:job:test")


if __name__ == '__main__':
    unittest.main()


