import subprocess, time

while 1:
    try:
        status = subprocess.call(['rq', 'info'])
    except KeyboardInterrupt:
        print 'ending...'
    finally:
        time.sleep(2)
