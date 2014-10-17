import requests
import json
import time
import base64

##### SCRIPT INSTRUCTIONS #####
# This script will replicate a set of databases owned by account1 to a new set of databases, with a 
# different Q value, owned by account2.
#
# 1) Enter in the credentials for account1/pwd1 and account2/pwd2.  They can be the same.
#
# 2) 'rename_dbs' --> Determine if you want to append a '2' to the new database names.  This option 
#		will likely be utilized when the accounts are the same and you need unique database names.
#
# 3) 'reshard_all' --> Determine whether you want to migrate ALL databases owned by account1 to a
#		new Q value.  If you only want to migrate a subset of the databases, set this variable to
#		false and utilize the 'db_list' for explicitly selecting databases.
#
# 4) 'skip_design_docs' --> Allows you to filter out design docs during the replication to prevent
#		index builds on the target database.
#
# 5) 'concurrency_limit' --> The script will ensure that it does not overload the cluster.  This
#		variable represents the upper limit for concurrent replications.  Use care when modifying.
#
# 6) 'new_q' --> The new shard configuration for the databases owned by account2.
#
# 7) 'repl_options' --> Use this variable for any extra replication settings you would like.  By
#		default, the replications are not continuous and we are not creating the database on the
#		target account.  If you decide to make the replications continuous, be sure to modify the
#		concurrency_limit to allow for more active replications.


account1 = 'FILL ME IN'
pwd1 = 'FILL ME IN'
account2 = 'FILL ME IN'
pwd2 = 'FILL ME IN'
auth1 = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(account1, pwd1)))
auth2 = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(account2, pwd2)))
rename_dbs = True
reshard_all = True
skip_design_docs = True
db_list = ['add', 'specific', 'databases', 'here']
concurrency_limit = 5
new_q = "12"
url1 = 'https://{0}.cloudant.com/'.format(account1)
url2 = 'https://{0}.cloudant.com/'.format(account2)
repl_options = {'continuous': False, 'create_target': False}




def get_dbs(url, auth):
	if reshard_all:
		r = requests.get('{0}_all_dbs'.format(url), headers={'Authorization': auth}).json()
		if '_replicator' in r:
			r.remove('_replicator')
		return r
	else:
		return db_list


def create_repl_doc(source, source_auth, target, target_auth, repl_options={}):
	doc = {
			'source': {
				'url': source,
				'headers': {
					'Authorization': source_auth
				}
			},
			'target': {
				'url': target,
				'headers': {
					'Authorization': target_auth
				}
			}
	}

	# include a filter function if we want to skip the design docs
	if skip_design_docs:
		doc = create_filter_func(source, source_auth, doc)

	# add the replication paramaters
	doc.update(repl_options)

	return doc


def create_filter_func(source, auth, doc, rev='', retries=5):
	# we've hit the base case, exit!
	if retries == 0:
		print 'Retry limit (5) exceeded.  Exiting...'
		exit()

	ddoc = {
			'_id': '_design/filtered_repl_for_reshard',
			'filters': {
						'skip_design_docs': 
							'function(doc, req) {if(doc._id.indexOf("_design") == 0) {return false;} else {return true;}}'
			}
	}

	# if the revision has been populated, include it in the design doc
	if rev:
		ddoc.update({'_rev': rev})

	# post the doc to cloudant
	r = requests.post(source, data=json.dumps(ddoc),
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# Handle potential errors
	if 'error' in r:

		# In the off chance the design doc already exists, let's give them the option to replace/update it.
		if r['error'] == 'conflict':
			prompt = 'The filtering design doc already exists for {0}.  Would you like to replace it? (Y/n):'.format(source)
			selection = raw_input(prompt)
			
			if not selection:
				selection = 'Y'
			if selection == 'Y' or selection == 'y':
				print 'Retrying...'
				
				ddoc_url = '{0}/_design/filtered_repl_for_reshard'.format(source)
				r = requests.get(ddoc_url, headers={'Authorization': auth}).json()
				
				if 'error' in r or '_rev' not in r:
					print 'Retry failed.  Skipping...'
				else:
					retries = retries - 1
					doc = create_filter_func(source, auth, doc, r['_rev'], retries)
			else:
				print 'Skipping filtered replication for {0}!'.format(source)

		# There was no conflict, but it still failed.  Should we continue anyway?
		else:
			print 'Failed creating a filtered replication ddoc for {0}!'.format(source)
			print json.dumps(r, indent=4)
			selection = raw_input('Would you like to continue anyway? (Y/n):')

			if not selection:
				selection = 'Y'
			if selection == 'Y' or selection == 'y':
				print 'Continuing...'
			else:
				exit()

	# There were no errors, success!  Let's update the replication doc with the filter name
	else:
		doc.update({'filter': 'filtered_repl_for_reshard/skip_design_docs'})
			
	# finally return the replication doc
	return doc


def create_new_db(url, db, auth):
	r = requests.put(url + db + '?q=' + new_q, headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			print 'Database {0} already exists, continuing...'.format(db)
		else:
			print 'Failed to create database {0}!  Exiting...'.format(db)
			exit()
	else:
		print 'Successfully created database {0}.'.format(db)


def post_repl_doc(url, doc, auth, retries=5):
	# Base case - we've run out of retries
	if retries == 0:
		print 'Failed to trigger replication after 5 retries.  Exiting...'
		exit()

	# post the doc
	r = requests.post(url + '_replicator', data=json.dumps(doc), 
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# check for errors
	if 'error' in r:
		if r['error'] != 'conflict':
			print 'Failed to post a replication doc to {0}.  Response:'.format(url)
			print json.dumps(r, indent=4)
			print 'Exiting...'
			exit()
		# Else, something happened between when we created the replication doc and
		# when we tried to post it.  Let's try to read it back and verify whether the
		# replication has triggered.

	# There's some odd behavior when posting to the _replicator.  The replication isn't always
	# triggered and can end up in "replication purgatory".  Read back the replication doc 
	# and ensure that it has triggered or completed.  If not, repost until successful.
	repl_doc_url = '{0}_replicator/{1}'.format(url, r['id'])
	time.sleep(1)
	r = requests.get(repl_doc_url, headers={'Authorization': auth}).json()
	if '_replication_state' not in r:
		# retry the post
		print 'Failed to trigger replication "{0}".  Retrying...'.format(r['id'])
		doc.update({'_rev': r['rev']})
		retries = retries - 1
		post_repl_doc(url, doc, auth, retries)


def poll_active_repl(url, auth, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeded.  Failed to retrieve _active_tasks.  Exiting...'
		exit()

	num_repl = 0
	tasks = requests.get(url + '_active_tasks', headers={'Authorization': auth}).json()

	# handle potential errors
	if 'error' in tasks:
		print 'Failed to retrieve _active_tasks.  Retrying...'
		print json.dumps(tasks, indent=4)
		retries = retries - 1
		return poll_active_repl(url, auth, retries)

	# iterate over each doc returned
	for doc in tasks:
		if doc['type'] == 'replication':
			num_repl += 1
	return num_repl


def create_replicator(url, auth):
	r = requests.put('{0}_replicator'.format(url), headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			print 'Database _replicator already exists, continuing...'
		else:
			print 'Failed to create database _replicator!  Exiting...'
			print json.dumps(r, indent=4)
			exit()
	else:
		print 'Successfully created database _replicator.'


def main():
	# locate all dbs for account1
	print 'Reading dbs requested for resharding...'
	dbs = get_dbs(url1, auth1)
	print 'Retrieved {0} dbs.  Beginning the resharding process...'.format(len(dbs))

	# create the _replicator db on the source if it doesn't already exist
	create_replicator(url1, auth1)

	# iterate through array of dbs and initiate replications to targets
	db_index = 0
	while db_index < len(dbs):

		# we only spawn new replications if it's below the limit.  Don't want
		# to overload the cluster. 
		if poll_active_repl(url1, auth1) < concurrency_limit:

			# db1 -> source db, db2 -> target db
			db1 = db2 = dbs[db_index]

			# make the target db name unique if required
			if rename_dbs:
				db2 += '2'

			# attempt to create the target db with the new q value
			create_new_db(url2, db2, auth2)

			# build a replication doc
			doc = create_repl_doc(url1 + db1, auth1, url2 + db2, auth2, repl_options)

			# post the doc to the source db (i.e. the mediator)
			post_repl_doc(url1, doc, auth1)
			print 'DB replication for {0} initiated...'.format(dbs[db_index])

			# increment index in to array of dbs
			db_index += 1

		else:
			# sleep for an arbitrary amount of time before polling again
			print 'Concurrent replication limit reached...waiting...'
			time.sleep(10)


if __name__ == "__main__":
	main()