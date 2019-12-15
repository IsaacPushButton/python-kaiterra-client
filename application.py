import schedule
import historypull
import time
import datetime
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

# minutes
pull_interval = 5
recheck_interval = 10


def pull_recent_history():
    logging.info("----------------------------------------------------------------")
    logging.info("Pulling history for all devices since last reading")
    logging.info("----------------------------------------------------------------")
    historypull.pull_history()
    logging.info("----------------------------------------------------------------")
    logging.info("Recent readings pulled. Next pull in {} minutes ".format(pull_interval))
    logging.info("----------------------------------------------------------------")


def recheck_period(hours):
    logging.info("----------------------------------------------------------------")
    logging.info("Rechecking data for the last {} hours".format(hours))
    logging.info("----------------------------------------------------------------")
    historypull.pull_history(hours)
    logging.info("----------------------------------------------------------------")
    logging.info("Recheck complete. Next recheck in {} minutes ".format(pull_interval))
    logging.info("----------------------------------------------------------------")


def recheck24():
    recheck_period(24)


#historypull.pull_history(24)
schedule.every(pull_interval).minutes.do(pull_recent_history)
schedule.every(recheck_interval).minutes.do(recheck24)
while True:
    schedule.run_pending()
    time.sleep(1)
