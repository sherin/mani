import unittest
import time
from threading import Thread

from mani import Mani

redis_url = "redis://localhost:6379/"

jobs_ran = []
mani = Mani(redis_url)

@mani.every(seconds=2)
def run_one_job():
    jobs_ran.append("run_one_job")

@mani.every(seconds=5)
def run_two_job():
    jobs_ran.append("run_two_job")

class TestScheduler(unittest.TestCase):

    def test_scheduler(self):
        t = Thread(target=mani.start)
        t.start()

        time.sleep(6)
        mani.stop()
        t.join()

        self.assertTrue(5 <= len(jobs_ran) <= 7)

        one_job_ran = filter(lambda j: j == "run_one_job", jobs_ran)
        self.assertTrue(3 <= len(one_job_ran) <= 4)

        two_job_ran = filter(lambda j: j == "run_two_job", jobs_ran)
        self.assertTrue(1 <= len(two_job_ran) <= 2)


    def tearDown(self):
        mani.redis.delete("mani:job:run_one_job")
        mani.redis.delete("mani:job:run_two_job")

