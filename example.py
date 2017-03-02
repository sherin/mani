
from mani import Mani

import pytz
import logging
logging.basicConfig(format='%(asctime)s %(message)s')


# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)

mani_log = logging.getLogger("mani")
mani_log.setLevel(logging.DEBUG)
mani_log.addHandler(ch)

redis_url = "redis://localhost:6379/"
config = {
  "timezone": pytz.timezone('US/Pacific')
}

mani = Mani(redis_url, config)

@mani.every(minutes=1)
def foo():
    print "running foo"

@mani.every(weeks=1, at="mon 19:00:00")
def bar():
    print "running bar"

@mani.every(days=1, at="11:30:00")
def baz():
    print "running bar"

mani.start()
