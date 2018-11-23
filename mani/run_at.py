import pytz
import re
from datetime import timedelta

from . import util

class RunAt:
    MINUTE_PATTERN = "(\d\d?):(\d\d?)"
    HOUR_PATTERN = "(\d\d?):(\d\d?):(\d\d?)"
    WEEKDAY_PATTERN = "(...) %s" % HOUR_PATTERN

    PATTERNS = [MINUTE_PATTERN, HOUR_PATTERN, WEEKDAY_PATTERN]

    WEEKDAY_MAP = {
        "mon": 0,
        "tue": 1,
        "wed": 2,
        "thu": 3,
        "fri": 4,
        "sat": 5,
        "sun": 6
    }

    def __init__(self, period, at, now, preferred_timezone, offset=None):
        self.period = period
        self.at = at
        self.offset = offset
        self.now = now
        self.preferred_timezone = preferred_timezone
        self.minute = None
        self.hour = None
        self.wday = None

    def parse(self):

        # weekday pattern
        regex = re.compile(self.WEEKDAY_PATTERN)
        match = re.match(regex, self.at)
        if match:
            self.wday = self.WEEKDAY_MAP.get(match.group(1).lower(), 0)
            self.hour = int(match.group(2))
            self.minute = int(match.group(3))
            return True

        # hour pattern
        regex = re.compile(self.HOUR_PATTERN)
        match = re.match(regex, self.at)
        if match:
            self.hour = int(match.group(1))
            self.minute = int(match.group(2))
            return True

        # minute pattern
        regex = re.compile(self.MINUTE_PATTERN)
        match = re.match(regex, self.at)
        if match:
            self.minute = int(match.group(1))
            return True

    def last_at(self):
        self.offset = self.now - timedelta(seconds=self.period)
        last_ran = self.next_at()
        if last_ran >= self.now:
            last_ran = last_ran - timedelta(seconds=self.period)
        return last_ran

    def next_at(self):
        run_at = self.offset + timedelta(seconds=self.period)
        if not self.at:
            return run_at

        matched = self.parse()
        if not matched:
            return self.offset

        utc_offset_hours = 0
        utc_offset_minutes = 0

        if self.preferred_timezone != pytz.utc:
            run_at_preferred_tz = run_at.astimezone(self.preferred_timezone)
            utc_offset_seconds = run_at_preferred_tz.utcoffset().total_seconds()
            utc_offset_hours = int(utc_offset_seconds / (60 * 60))
            utc_offset_minutes = int((utc_offset_seconds - (utc_offset_hours * 60 * 60))/60)

        if self.minute is not None:
            minute = self.minute - utc_offset_minutes
            run_at = run_at.replace(minute=minute)
        if self.hour is not None:
            hours_added = self.hour - utc_offset_hours
            days_added = int(hours_added / 24) # when hours added > 24, then adjust the date
            run_at += timedelta(days=days_added)
            hour = hours_added % 24
            run_at = run_at.replace(hour=hour)
        if self.wday is not None:
            run_at = util.next_weekday(self.wday, run_at)

        return run_at
