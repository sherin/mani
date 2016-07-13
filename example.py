
from mani import Mani

import logging

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

@mani.every(minutes=1)
def foo():
    print 1 + 1

mani.start()
