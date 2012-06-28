import pylibaio
import os
import sys
import time

BLOCK_SIZE = 64 << 10
NUM_WRITES = 1 << 14
FILE_BLOCKS = 1 << 14

MAX_IN_FLIGHT = 254
MIN_EVENTS_TO_REAP = 50

BLOCKS = [chr(i) * BLOCK_SIZE for i in xrange(min(256, FILE_BLOCKS))]

def main():
	fname = sys.argv[1]
	fd = os.open(fname, os.O_CREAT | os.O_RDWR | os.O_DIRECT)

	writes = [((i % FILE_BLOCKS) * BLOCK_SIZE, BLOCKS[i % len(BLOCKS)]) for i in xrange(NUM_WRITES)]
	
	in_flight = 0
	scheduled = 0
	events_left = NUM_WRITES
	start_time = time.time()
	while scheduled < NUM_WRITES:
		sys.stdout.write('Scheduled %d/%d\r' % (scheduled, NUM_WRITES))
		sys.stdout.flush()
		if in_flight < MAX_IN_FLIGHT:
			sched_this_round = pylibaio.write(fd, writes[scheduled : scheduled + MAX_IN_FLIGHT - in_flight])
			if sched_this_round <= 0:
				continue
			scheduled += sched_this_round
			in_flight += sched_this_round
		else:
			events = pylibaio.get_events(min(events_left, MIN_EVENTS_TO_REAP), MAX_IN_FLIGHT)
			events_left -= len(events)
			in_flight -= len(events)
	
	while events_left > 0:
		events = pylibaio.get_events(min(MIN_EVENTS_TO_REAP, events_left), MAX_IN_FLIGHT)
		events_left -= len(events)
	total_time = time.time() - start_time
	total_data = (BLOCK_SIZE * NUM_WRITES) >> 20
	print
	print 'Wrote %d MB took %.3f seconds (%.3f MB/sec)' % (total_data, total_time, total_data / total_time)

if __name__ == '__main__':
	main()
