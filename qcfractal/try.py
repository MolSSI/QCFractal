from apscheduler.schedulers.background import BackgroundScheduler
import time


def run_now():
    print('**** in run now')

def run_later():
    print('**** run later')

def run_later2():
    print('=============== add  after start')

scheduler = BackgroundScheduler(daemon=False)
scheduler.add_job(run_now, 'date')
scheduler.add_job(run_later, 'interval', minutes=0.2)

time.sleep(3)
print('after first sleep.. now staring scheduler....')
scheduler.start()
time.sleep(3)
print('after second sleep')
scheduler.add_job(run_later2, 'date')
