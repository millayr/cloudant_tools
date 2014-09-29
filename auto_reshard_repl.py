import requests
import json
import time

source_uname = 'millayr'
source_pwd = 'VU_class11'
source_auth = (source_uname, source_pwd)
target_uname = 'millayr-test'
target_pwd = 'not4long'
target_auth = (target_uname, target_pwd)
new_dbs = False
concurrency_limit = 5
new_q = "8"
source_url = 'https://{0}.cloudant.com/'.format(source_uname)
target_url = 'https://{0}.cloudant.com/'.format(target_uname)
repl_options = {'continuous': False, 'create_target': False}

def get_dbs(url, auth):
	#return requests.get(url + '_all_dbs', auth=auth.json()
	return ["foundbite", "crud"]

def create_repl_doc(source, target, db1, db2, repl_options={}):
	doc = {
			'source': {'url': source + db1},
			'target': {'url': target + db2}
	}
	doc.update(repl_options)
	return doc

def create_new_db(target, db, auth):
	r = requests.put(target + db + "?q=" + new_q, auth=auth)

def poll_active_repl(url, auth):
	num_repl = 0
	tasks = requests.get(url + '_active_tasks', auth=auth).json()
	print json.dumps(tasks, indent=4)
	# iterate over each doc returned
	for doc in tasks:
		if doc['type'] == 'replication':
			num_repl += 1
	return num_repl

def main():
	# locate all dbs for the account
	dbs = get_dbs(source_url, source_auth)
	print json.dumps(dbs, indent=4)

	# iterate through array of dbs and initiate replications to targets
	db_index = 0
	while db_index < len(dbs):

		# we only spawn new replications if it's below the limit.  Don't want
		# to overload the cluster. 
		if poll_active_repl(source_url, source_auth) < concurrency_limit:
			source_db = target_db = dbs[db_index]
			if new_dbs:
				target_db += '2'

			create_new_db(target_url, target_db, target_auth)
			doc = create_repl_doc(source_url, 
									target_url, 
									source_db, 
									target_db, 
									repl_options)
			requests.post(source_url + '_replicator', 
							data=json.dumps(doc), 
							headers={'content-type': 'application/json'},
							auth=source_auth)
			print 'DB replication for {0} initiated...'.format(dbs[db_index])
			db_index += 1
		else:
			# sleep for an arbitrary amount of time before polling again
			print 'Concurrent replication limit reached...waiting...'
			time.sleep(10)

if __name__ == "__main__":
	main()