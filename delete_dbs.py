import requests
import json
import multiprocessing.dummy as multiprocessing
import uuid

account = 'FILL ME IN'
pwd = 'FILL ME IN'
num_threads = 10

s = requests.Session()


def slice_array(array, size):
	if size < 1:
		size = 1
	return [array[i:i + size] for i in range(0, len(array), size)]


def delete_dbs(dbs):
	for db in dbs:
		r = s.delete('https://{0}.cloudant.com/{1}'.format(account, db), auth=(account, pwd)).json()
		print 'Deleted {0}...'.format(db)


def main():
	dbs = s.get('https://{0}.cloudant.com/_all_dbs'.format(account), auth=(account, pwd)).json()
	if '_replicator' in dbs:
		dbs.remove('_replicator')

	range_size = len(dbs) // num_threads
	db_chunks = slice_array(dbs, range_size)

	threads = []
	for chunk in db_chunks:
		t = multiprocessing.Process(target=delete_dbs, args=(chunk,))
		threads.append(t)
		t.start()

	for t in threads:
		t.join()

if __name__ == "__main__":
	main()
