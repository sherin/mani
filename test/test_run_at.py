import pytz
import unittest
from datetime import datetime, timedelta
from freezegun import freeze_time

from mani.run_at import RunAt

jobs_ran = []
def run_job():
    jobs_ran.append(1)

def display(utc_dt, tz_dt):
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'
    print "utc time: %s" % utc_dt.strftime(fmt)
    print "local time: %s" % tz_dt.strftime(fmt)

def pretty_fmt(dt):
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'
    return dt.strftime(fmt)

def utc_offset_in_hours(dt):
    return dt.utcoffset().total_seconds() / (60 * 60)

class TestRunAtDSTIn(unittest.TestCase):
    config = {
      "timezone": pytz.timezone('US/Pacific')
    }

    def test_init(self):
        # PT - March 11, 1:59 am (+8 UTC). At 2 am, DST kicks in moving time 1 hr forward (+7 UTC)
        dt = datetime(2018, 3, 11, 9, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            tz_now = now.astimezone(self.config['timezone'])
            display(now, tz_now)
            self.assertEqual(utc_offset_in_hours(tz_now), -8)

            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            tz_now = now.astimezone(self.config['timezone'])
            display(now, tz_now)
            self.assertEqual(utc_offset_in_hours(tz_now), -7)

    def test_minutely(self):
        # PT - March 11, 1:59 am (+8 UTC). At 2 am, DST kicks in moving time 1 hr forward (+7 UTC)
        dt = datetime(2018, 3, 11, 9, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            last_ran = datetime.utcnow().replace(tzinfo=pytz.utc)
            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            run_at = RunAt(60, None, now, self.config['timezone'], offset=last_ran).next_at()
            self.assertEqual(run_at, now)

    def test_daily(self):
        # PT - March 11, 1:59 am (+8 UTC). At 2 am, DST kicks in moving time 1 hr forward (+7 UTC)
        dt = datetime(2018, 3, 11, 9, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            last_ran = datetime(2018, 3, 10, 18, 0, tzinfo=pytz.utc) # Ran at 10 am on 10th March PT
            frozen_time.tick(delta=timedelta(seconds=60))

            one_day = 24 * 60 * 60
            run_at = RunAt(one_day, "10:00:00", now, self.config['timezone'], offset=last_ran).next_at()

            # assert that runs-at is at 10 am PT
            self.assertEqual(pretty_fmt(run_at.astimezone(self.config['timezone'])), "2018-03-11 10:00:00 PDT-0700")
            # assert that runs-at is at 17 HH UTC
            self.assertEqual(pretty_fmt(run_at), "2018-03-11 17:00:00 UTC+0000")

class TestRunAtDSTOut(unittest.TestCase):
    config = {
      "timezone": pytz.timezone('US/Pacific')
    }

    def test_init(self):
        # PT - Nov 4, 1:59 am (+7 UTC). At 2 am, DST kicks in moving time 1 hr behind (+8 UTC)
        dt = datetime(2018, 11, 4, 8, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            tz_now = now.astimezone(self.config['timezone'])
            display(now, tz_now)
            self.assertEqual(utc_offset_in_hours(tz_now), -7)

            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            tz_now = now.astimezone(self.config['timezone'])
            display(now, tz_now)
            self.assertEqual(utc_offset_in_hours(tz_now), -8)

    def test_minutely(self):
        # PT - Nov 4, 1:59 am (+7 UTC). At 2 am, DST kicks in moving time 1 hr behind (+8 UTC)
        dt = datetime(2018, 11, 4, 8, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            last_ran = datetime.utcnow().replace(tzinfo=pytz.utc)
            frozen_time.tick(delta=timedelta(seconds=60))

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            run_at = RunAt(60, None, now, self.config['timezone'], offset=last_ran).next_at()
            self.assertEqual(run_at, now)

    def test_daily(self):
        # PT - Nov 4, 1:59 am (+7 UTC). At 2 am, DST kicks in moving time 1 hr behind (+8 UTC)
        dt = datetime(2018, 11, 4, 8, 59, tzinfo=pytz.utc)

        with freeze_time(dt) as frozen_time:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            last_ran = datetime(2018, 11, 3, 17, 0, tzinfo=pytz.utc) # Ran at 10 am on 4th Nov
            frozen_time.tick(delta=timedelta(seconds=60))

            one_day = 24 * 60 * 60
            run_at = RunAt(one_day, "10:00:00", now, self.config['timezone'], offset=last_ran).next_at()

            # assert that runs-at is at 10 am PT
            self.assertEqual(pretty_fmt(run_at.astimezone(self.config['timezone'])), "2018-11-04 10:00:00 PST-0800")
            # assert that runs-at is at 18 HH UTC
            self.assertEqual(pretty_fmt(run_at), "2018-11-04 18:00:00 UTC+0000")

if __name__ == '__main__':
    unittest.main()

