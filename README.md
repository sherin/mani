# Mani [![Build Status](https://travis-ci.org/sherinkurian/mani.svg?branch=master)](https://travis-ci.org/sherinkurian/mani)

Mani is a distribued cron like scheduler. It uses redis to acquire lock on jobs (ensuring a job runs on one node only) and determining when to run the job next. 

## Installation

`pip install mani`

## Usage

```
from mani import Mani
clock = Mani("redis://localhost:6379/")

@clock.every(minutes=1)
def foo():
  print "running foo every minute"

@clock.every(weeks=1, at="mon 19:00:00")
def bar():
  print "running bar every week on monday"

@clock.every(hours=1, at="25:00")
def baz():
  print "running baz hourly on the 25th minute"

@clock.every(days=1, at="13:00:00")
def qux():
  print "running qux daily at 1 pm"
```

### Run on specific timezone

```
import pytz
from mani import Mani

config = {
  "timezone": pytz.timezone('US/Pacific')
}
clock = Mani("redis://localhost:6379/", config)

@clock.every(minutes=1)
def foo():
  print "running foo every minute"

@clock.every(weeks=1, at="mon 19:00:00")
def bar():
  print "running bar every week on monday at 7 pm Pacific time"
```
