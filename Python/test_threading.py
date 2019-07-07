import time
from threading import Thread


def countdown(n):
    while n > 0:
        print 'T-minus{}'.format(n)
        n -= 1
        time.sleep(2)

t = Thread(target=countdown, args=(10,))
# t.daemon = True
t.start()
print('hello')

if t.is_alive():
    print 'Still running'
else:
    print 'Completed'


