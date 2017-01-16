
from mani import Mani

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
mani = Mani(redis_url)

@mani.every(hours=1, at="25:00")
def foo():
    print "running foo"

@mani.every(weeks=1, at="mon 19:00:00")
def bar():
    print "running bar"

mani.start()
