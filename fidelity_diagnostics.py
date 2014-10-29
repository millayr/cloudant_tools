# Author:  Ryan Millay (rmillay@us.ibm.com)
# Diagnostic tool for multihomed topology.

import requests
import getpass
import json
import time

account1 = ''
pwd1 = ''
account2 = ''
pwd2 = ''
multihomed_account = 'fidsafe'
cluster1 = 'fidelity001'
cluster2 = 'fidelity002'
dbs = ['fidsafe_dev', 'fidsafe_prod0']
scrubbing = True
pending_changes_enabled = False


# Retrieve the credentials from the user.  Both sets of credentials are
# saved to the global parameters.
def get_credentials():
	global account1
	global account2
	global pwd1
	global pwd2

	valid_creds1 = False
	valid_creds2 = False

	while not valid_creds1:
		while not account1:
			prompt = 'Enter your Cloudant account for {0}: '.format(cluster1)
			account1 = raw_input(prompt)

		while not pwd1:
			pwd1 = getpass.getpass('Enter the password: ')

		# perform test request to validate credentials
		if not test_creds((account1, pwd1), cluster1):
			account1 = ''
			pwd1 = ''
		else:
			valid_creds1 = True

	while not valid_creds2:
		while not account2:
			prompt = 'Enter your Cloudant account for {0}: '.format(cluster2)
			account2 = raw_input(prompt)

		while not pwd2:
			pwd2 = getpass.getpass('Enter the password: ')

		# perform test request to validate credentials
		if not test_creds((account2, pwd2), cluster2):
			account2 = ''
			pwd2 = ''
		else:
			valid_creds2 = True


# verify a set of credentials against a given cluster
def test_creds(auth, cluster):
	url = 'https://{0}.cloudant.com/'.format(cluster)
	r = requests.get(url, auth=auth, headers={"x-cloudant-user": multihomed_account})

	if r.status_code != 200 and r.status_code != 401:
		print '!! Connection to Cloudant failed !!'
		print r
		print r.text
		exit()

	if 'error' in r.json():
		print '!! WARNING !! --> Credentials are invalid.  Please try again.'
		return False
	else:
		return True


# Return all documents from the _replicator DB from the given cluster.
def get_repl_docs(cluster, auth):
	print '\n### Retrieving replication docs for {0}...\n'.format(cluster)
	url = 'https://{0}.cloudant.com/_replicator/_all_docs?include_docs=true'.format(cluster)
	r = requests.get(url, auth=auth, headers={"x-cloudant-user": multihomed_account})

	if r.status_code == 403:
		raise Exception('!! Please ensure your credentials have sufficient privileges to access this information !!', r)
	if r.status_code != 200:
		raise Exception('!! Connection to Cloudant failed !!', r)

	return r.json()



# Given a set of docs from the _replicator DB on a specific cluster, output
# all failures.  If no failures exist, notify the user of a success.  Optionally display
# the doc list.
def parse_repl_docs(json_response, cluster):
	rows = json_response['rows']
	error = False

	# cycle through each replication doc
	for row in rows:
		doc = row['doc']

		# inspect the urls and scrub out any credentials
		if scrubbing:
			target_url = doc['target']['url']
			if '@' in target_url:
				target_url = 'https://<** scrubbed **>@' + doc['target']['url'].split('@',1)[1]
				doc['target']['url'] = target_url

			source_url = doc['source']['url']
			if '@' in source_url:
				source_url = 'https://<** scrubbed **>@' + doc['source']['url'].split('@',1)[1]
				doc['source']['url'] = source_url

		# check for any replication errors
		if doc['_replication_state'] == 'error':
			print '!! WARNING !!  -->  Replication doc {0} has failed!'.format(doc['_id'])
			error = True

	# print success message if we did not encounter any errors in the replications
	if not error:
		print '!! SUCCESS !! --> No errors were found in the replication docs for {0}!'.format(cluster)

	# print all replication docs if requested by the user
	prompt = '\nWould you like to see the complete set of replication docs for {0}? (Y/n): '.format(cluster)
	selection = raw_input(prompt)
	if not selection:
		selection = 'Y'

	if selection == 'Y' or selection == 'y':
		time.sleep(2)
		print json.dumps(json_response, indent=4) + '\n'



# For both clusters, iterate through all replications and notify the user
# of any failures.
def get_repl_status():
	
	# cluster 1
	try:
		r = get_repl_docs(cluster1, (account1, pwd1))
		time.sleep(2)

		# iterate through each doc and verify it's status
		parse_repl_docs(r, cluster1)
	except Exception as e:
		msg, response = e.args
		print msg
		print response
		print response.text

	raw_input('\nPress any key to continue...')

	#cluster 2
	try:
		r = get_repl_docs(cluster2, (account2, pwd2))
		time.sleep(2)

		# iterate through each doc and verify it's status
		parse_repl_docs(r, cluster2)
	except Exception as e:
		msg, response = e.args
		print msg
		print response
		print response.text



# Given a db, cluster, and credentials, return the high level stats doc for the db
def get_db_stats(db, cluster, auth):
	print '### Retrieving database statistics for {0} on {1}...'.format(db, cluster)
	url = 'https://{0}.cloudant.com/{1}'.format(cluster, db)
	r = requests.get(url, auth=auth, headers={"x-cloudant-user": multihomed_account})

	if r.status_code == 403:
		raise Exception('!! Please ensure your credentials have sufficient privileges to access this information !!', r)
	if r.status_code != 200:
		raise Exception('!! Connection to Cloudant failed !!', r)

	return r.json()



# For a given db, compare the doc counts on both clusters.  Output either
# success or failure to the user.
def compare_db_stats(db):
	try:
		# cluster 1
		r1 = get_db_stats(db, cluster1, (account1, pwd1))
		time.sleep(2)

		# cluster 2
		r2 = get_db_stats(db, cluster2, (account2, pwd2))
		time.sleep(2)

		# difference in total docs
		doc_diff = abs(r1['doc_count'] - r2['doc_count'])
		# difference in deleted docs
		del_doc_diff = abs(r1['doc_del_count'] - r2['doc_del_count'])
		print ''

		error = False
		if doc_diff != 0:
			print '!! WARNING !! --> The number of documents for {0} is off by {1}!'.format(db, doc_diff)
			error = True

		if del_doc_diff != 0:
			print '!! WARNING !! --> The number of deleted documents for {0} is off by {1}!'.format(db, del_doc_diff)
			error = True

		if not error:
			print '!! SUCCESS !! --> The number of documents match for {0}!'.format(db)
		
		# print all stats docs if requested by the user
		prompt = '\nWould you like to see the database statistics for {0} on {1} and {2}? (Y/n): '.format(db, cluster1, cluster2)
		selection = raw_input(prompt)
		if not selection:
			selection = 'Y'

		if selection == 'Y' or selection == 'y':
			time.sleep(2)
			print '\n# {0} on {1}\n'.format(db, cluster1)
			print json.dumps(r1, indent=4) + '\n'
			print '# {0} on {1}\n'.format(db, cluster2)
			print json.dumps(r2, indent=4)

	except Exception as e:
		msg, response = e.args
		print msg
		print response
		print response.text



def get_pending_repl(cluster, auth):
	print '\n### Retrieving pending replication changes for {0}...\n'.format(cluster)
	url = 'https://{0}.cloudant.com/_active_tasks'.format(cluster)
	r = requests.get(url, auth=auth, headers={"x-cloudant-user": multihomed_account})

	if r.status_code == 403:
		print '!! Please ensure your credentials have sufficient privileges to access this information !!'
		print r
		print r.text
	elif r.status_code != 200:
		print '!! Connection to Cloudant failed !!'
		print r
		print r.text
	else:
		for doc in r.json():
			if doc['type'] == 'replication' and doc['doc_id'] is not None:
				pending = doc['changes_pending']
				repl_id = doc['doc_id']
				if pending == 0:
					print '!! SUCCESS !! --> "{0}" has {1} changes pending.'.format(repl_id, pending)
				else:
					print '!! WARNING !! --> "{0}" has {1} changes pending.'.format(repl_id, pending)
				time.sleep(2)



def get_pending_changes():
	# cluster 1
	get_pending_repl(cluster1, (account1, pwd1))
	raw_input('\nPress any key to continue...')
	get_pending_repl(cluster2, (account2, pwd2))



def main():
	# get credentials from the user
	get_credentials()

	# get replication status from both clusters
	get_repl_status()

	if pending_changes_enabled:
		# get the pending replication changes
		raw_input('\n\nDONE.  Press any key to continue with pending replication changes...')
		get_pending_changes()

	# get database statistics
	raw_input('\n\nDONE.  Press any key to continue with DB statistics...');
	print ''
	for db in dbs:
		compare_db_stats(db)
		raw_input('\nPress any key to continue...')
		print ''

	print '\nALL TESTS COMPLETE\n'

if __name__ == "__main__":
	main()