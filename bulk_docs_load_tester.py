import json
import requests
import multiprocessing.dummy as multiprocessing
import time

# variables
user = 'FILL ME IN'
pwd = 'FILL ME IN'
db = 'bulk_tester'
base_url = 'https://{0}.cloudant.com/'.format(user)
bulk_url = '{0}{1}/_bulk_docs'.format(base_url, db)
runs = [{'body_size': 1000, 'trials': 3}]
num_threads = 10
interval = 900 # in seconds
# sample file of doc
filename = '/FILL/ME/IN'
# design doc if needed
ddoc_filename = '/FILL/ME/IN/IF/NEEDED'
# output file for test results
output_file = '/FILL/ME/IN'
doc = ''


def execute(q, payload):
	s = requests.Session()

	# count the number of executions
	count = 0

	# create a stop time for this run
	timeout = time.time() + interval

	while True:
		if time.time() > timeout:
			break

		s.post(bulk_url, data=json.dumps(payload), headers={'content-type': 'application/json'}, auth=(user, pwd), verify=False)
		count += 1

	# add an item to the queue indicating the number of requests sent
	q.put(count)


def main():
	print 'Starting bulk loading program...'
	print 'List of test runs: {0}'.format(runs)
	print 'Output file: {0}'.format(output_file)

	with open(output_file, 'r+') as title:
		title.write('Time,Threads,Request Size,Requests/Second,Docs Written')

	for run in runs:
		body_size = run['body_size']
		trials = run['trials']

		for trial in range(trials):
			# read the external json file
			print 'Reading {0} for test document to post...'.format(filename)
			with open(filename) as fh:
				doc = json.load(fh)
			time.sleep(1)

			# build the request body we want to send 
			print 'Building a {0} document request body...'.format(body_size)
			request_body = {'docs': []}
			for i in range(body_size):
				request_body['docs'].append(doc)
			time.sleep(1)

			# delete the database if it exists and then recreate it
			print 'Re-initializing the testing database...'
			r = requests.get(base_url + '_all_dbs', auth=(user, pwd), verify=False).json()
			if db in r:
				requests.delete(base_url + db, auth=(user, pwd), verify=False)
			requests.put(base_url + db, auth=(user, pwd), verify=False)

			# insert ddocs if needed
			if ddoc_filename != '':
				print 'Posting ddocs to the database...'
				with open(ddoc_filename) as df:
					ddocs = json.load(df)
					requests.post(bulk_url, data=json.dumps(ddocs), headers={'content-type': 'application/json'}, auth=(user, pwd), verify=False)
			time.sleep(1)


			# start to create threads
			print 'Initializing {0} threads...'.format(num_threads)
			threads = []
			q = multiprocessing.Queue()
			for j in range(num_threads):
				t = multiprocessing.Process(target=execute, args=(q, request_body))
				threads.append(t)

			print '{0} threads created...'.format(num_threads)

			# start the threads
			for thread in threads:
				thread.start()

			print 'All threads started...'

			# wait until they are done
			for k in threads:
				k.join()

			# sum total docs written from the queue
			total_requests = 0
			while not q.empty():
				total_requests += q.get()

			# compute metrics
			requests_per_second = float(total_requests) / interval
			docs_written = total_requests * body_size

			with open(output_file, 'a') as oh:
				oh.write('\n{0},{1},{2},{3},{4}'.format(interval, num_threads, body_size, requests_per_second, docs_written))

			print 'Trial complete...'
			time.sleep(120)

		print 'TEST RUN COMPLETE'


if __name__ == "__main__":
	main()
