import pylibaio
import os

BUF_SIZE = 10
NUM_OPS = 10

def main():
	fd = os.open('a.dat', os.O_CREAT | os.O_RDWR)

	writes = [(chr(i) * BUF_SIZE, i * BUF_SIZE) for i in xrange(NUM_OPS)]
	reads = [(BUF_SIZE * i, BUF_SIZE) for i in xrange(NUM_OPS)]

	print 'write:', pylibaio.write(fd, writes)
	print 'read:', pylibaio.read(fd, reads)

	events_left = NUM_OPS * 2
	while events_left > 0:
		events = pylibaio.get_events(events_left)
		print len(events)
		print events
		events_left -= len(events)

if __name__ == '__main__':
	main()
