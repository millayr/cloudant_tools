import requests
import json
import multiprocessing.dummy as multiprocessing
import uuid

num_dbs = 20
num_docs = 5000
account = 'FILL ME IN'
pwd = 'FILL ME IN'
num_threads = 5
s = requests.Session()

def create_dbs(docs):
	for i in range(num_dbs):
		db = uuid.uuid1()
		r = s.put('https://{0}.cloudant.com/db-{1}'.format(account, db), auth=(account, pwd)).json()
		r = s.post('https://{0}.cloudant.com/db-{1}/_bulk_docs'.format(account, db), 
			headers={'Content-Type': 'application/json'}, auth=(account, pwd), data=json.dumps(docs)).json()
		print 'POSTed docs to db-{0}...'.format(db)


def main():
	doc = {'docs': []}

	for i in range(num_docs):
		doc['docs'].append({'Years': 18, 'Name': 'Angel', 'Job': 'Clerk', 'Dept': 93, 'Salary': 53055})

	print json.dumps(doc, indent=4)
	threads = []
	for i in range(num_threads):
		t = multiprocessing.Process(target=create_dbs, args=(doc,))
		threads.append(t)
		t.start()

	for t in threads:
		t.join()

if __name__ == "__main__":
	main()
