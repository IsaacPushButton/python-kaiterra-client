import schedule
import historypull
import time
import datetime
import logging

logging.basicConfig(filename='./collector.log', format='%(asctime)s %(message)s', level=logging.DEBUG)

pull_interval = 2


def pull_history():
    historypull.pull_history()
    logging.info("History pulled. Next pull in {} minutes ".format(pull_interval))


historypull.pull_history()
schedule.every(pull_interval).minutes.do(pull_history)

while True:
    schedule.run_pending()
    time.sleep(1)
