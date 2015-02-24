import requests
import json
import multiprocessing.dummy as multiprocessing
import uuid
import time

account = 'rgrbackup2'
user = 'adm-ryanmillay'
pwd = 'FILL ME IN'
num_threads = 20
output_dir = './all_docs_output_{0}'.format(time.time())
s = requests.Session()


def slice_array(array, size):
	if size < 1:
		size = 1
	return [array[i:i + size] for i in range(0, len(array), size)]


def stream_all_docs(dbs):
	for db in dbs:
		

		r = s.get('https://{0}.cloudant.com/{1}/_all_docs?include_docs=true'.format(account, db), auth=(user, pwd), stream=True)
			
		with open("{0}/{1}.json".format(output_dir, db), 'wb') as f:
			for chunk in r.iter_content(chunk_size=5000000):
				if chunk:
					f.write(chunk)
					f.flush()

		print 'Saved {0}...'.format(db)


def main():

	if not os.path.exists(output_dir):
		os.makedirs(output_dir)

	dbs = s.get('https://{0}.cloudant.com/_all_dbs'.format(account), auth=(user, pwd)).json()
	if '_replicator' in dbs:
		dbs.remove('_replicator')

	range_size = len(dbs) // num_threads
	db_chunks = slice_array(dbs, range_size)

	threads = []
	for chunk in db_chunks:
		t = multiprocessing.Process(target=stream_all_docs, args=(chunk,))
		threads.append(t)
		t.start()

	for t in threads:
		t.join()

if __name__ == "__main__":
	main()