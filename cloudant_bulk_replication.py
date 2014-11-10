# Author:  Ryan Millay, SE - Cloudant
# Initiate a bulk list of replications between account(s).  Allows for migrating a set of dbs to a new q.

import requests
import json
import time
import base64
import logging
import uuid

##### SCRIPT INSTRUCTIONS #####
# This script will replicate a set of databases owned by account1 to a new set of databases owned
# by account2.  The script allows for changes to Q values if desired.
#
# 1) Enter in the credentials for account1/pwd1 and account2/pwd2.  They can be the same.
#
# 2) 'rename_dbs' --> Determine if you want to append a '2' to the new database names.  This option 
#		will likely be utilized when the accounts are the same and you need unique database names.
#
# 3) 'repl_all' --> Determine whether you want to migrate ALL databases owned by account1 to a
#		new Q value.  When set to True, the script will get _all_dbs from account1 and mark them all
#		as slated for replication.  Also, when set to True, the 'db_list' variable is not used.
#
# 4) 'db_list' --> An array of db names owned by account1 that you would like replicated over to
#		account2.  Note that when 'repl_all' is set to True, this field is ignored.
#
# 5) 'skip_ddocs' --> Allows you to filter out design docs during the replication to prevent
#		index builds on the target database.  Set to True to enable.
#
# 6) 'skip_ddocs_name' --> String representing the ddoc necessary for filtering out ddocs during
#		a replication.  Change only if you know what you are doing.
#
# 7) 'skip_ddocs_func' --> String representing the filter function name.  Change only if you know
#		what you are doing.
#
# 8) 'concurrency_limit' --> The script will ensure that it does not overload the cluster.  This
#		variable represents the upper limit for concurrent replications.  Use care when modifying.
#
# 9) 'use_default_q' --> When set to True, databases on account2 will be created using the cluster
#		default shard configuration.  When set to False, databases on account2 will be created
#		with a new q value determined by 'new_q'.
#
# 10) 'new_q' --> The new shard configuration for the databases owned by account2.  This field is
#		ignored when 'use_default_q' is set to True.
#
# 11) 'repl_options' --> Use this variable for any extra replication settings you would like.  By
#		default, the replications are not continuous and we are not creating the database on the
#		target account.  If you decide to make the replications continuous, be sure to modify the
#		concurrency_limit to allow for more active replications.
#
# 12) 'force_triggered' --> When set to True, the script will attempt to verify all posted
#		replications have either triggered or completed.
#
# 13) 'retry_delay' --> Number of seconds to sleep before rechecking a replication that has not
#		triggered or completed yet.


##### EDITABLE VARIABLES #####
account1 = 'millayr'
pwd1 = 'not4long'
account2 = 'millayr-test'
pwd2 = 'not4long'
rename_dbs = False
repl_all = True
db_list = ['add', 'specific', 'databases', 'here']
skip_ddocs = False
skip_ddocs_name = 'filter_out_ddocs'
skip_ddocs_func = 'skip_ddocs'
concurrency_limit = 5
use_default_q = True
new_q = "8"
repl_options = {'continuous': False, 'create_target': True}
force_triggered = True
retry_delay = 5

##### DO NOT EDIT #####
auth1 = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(account1, pwd1)))
auth2 = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(account2, pwd2)))
url1 = 'https://{0}.cloudant.com/'.format(account1)
url2 = 'https://{0}.cloudant.com/'.format(account2)
num_failed_ddocs = 0
num_failed_repl = 0
logging.basicConfig(filename='replication.log',level=logging.DEBUG)


# Exception classes
class GeneralError(Exception):
	def __init__(self, msg, level):
		self.msg = msg
		self.level = level

class ResponseError(Exception):
	def __init__(self, msg, level, r):
		self.msg = msg
		self.level = level
		self.r = r

class FatalError(Exception):
	def __init__(self, msg, level):
		self.msg = msg
		self.level = level


# Accepts:  1) A URL to a cloudant account, 
#			2) the authorization header
# Returns:  An array of all databases slated for replication from account1 to account2
def get_dbs(url, auth):
	r = db_list

	if repl_all:
		r = requests.get('{0}_all_dbs'.format(url), headers={'Authorization': auth}).json()
		if '_replicator' in r:
			r.remove('_replicator')

	logging.info('GET_DBS: Databases slated for replication:')
	logging.info(json.dumps(r, indent=4))

	return r



# Accepts:  1) The source URL (both Cloudant account and db),
#			2) The source authorization header,
#			3) The target URL (both Cloudant account and db),
#			4) The target authorization header,
#			5) Any additional replication options (defaults to empty)
# Returns:  The prepared replication document
def create_repl_doc(source, source_auth, target, target_auth, repl_options={}):
	name = uuid.uuid1()

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
			},
			'user_ctx': {
				'name': account1,
				'roles': ['_admin', '_reader', '_writer']
			},
			'_id': 'cloudant_bulk_replication_{0}'.format(name)
	}

	# add the replication paramaters
	doc.update(repl_options)

	return doc



# Accepts:  1) The source URL (both the Cloudant account and db),
#			2) The authorization header,
#			3) The revision of the new filter function design doc (defaults to empty),
#			4) The number of retries remaining (defaults to 5)
# Returns:  Void
def create_filter_func(source, auth, rev='', retries=5):
	# we've hit the base case, exit!
	if retries == 0:
		global num_failed_ddocs
		num_failed_ddocs += 1
		print 'Retry limit (5) exceeded.  Skipping...'
		raise GeneralError('CREATE_FILTER_FUNC: Retry limit (5) exceeded for {0}.  No ddoc created!'.format(source), logging.ERROR)

	ddoc = {
			'_id': '_design/{0}'.format(skip_ddocs_name),
			'filters': {
						'{0}'.format(skip_ddocs_func): 
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
		return filter_func_error_handle(r, source, auth, retries)



# Accepts:  1) The json response object,
#			2) The source URL that returned the error,
#			3) The authorization header for the source,
#			4) The number of retries left for this filter function creation
# Returns:	Void
def filter_func_error_handle(response, source, auth, retries):
	global num_failed_ddocs

	# In the off chance the design doc already exists, let's give them the option to replace/update it.
	if response['error'] == 'conflict':
		logging.info('FILTER_FUNC_ERROR_HANDLE: There was a conflict for filter ddoc for {0}.'.format(source))
		prompt = 'The filtering design doc already exists for {0}.  Would you like to replace it? (Y/n):'.format(source)
		selection = raw_input(prompt)
		
		if not selection:
			selection = 'Y'
		if selection == 'Y' or selection == 'y':
			print 'Retrying...'
			
			ddoc_url = '{0}/_design/filtered_repl_for_reshard'.format(source)
			r = requests.get(ddoc_url, headers={'Authorization': auth}).json()
			
			if 'error' in r or '_rev' not in r:
				num_failed_ddocs += 1
				print 'Retry failed.  Skipping...'
				raise ResponseError('FILTER_FUNC_ERROR_HANDLE: Failed when retrying filter ddoc post for {0}!'.format(source), logging.ERROR, r)
			else:
				retries -= 1
				logging.info('FILTER_FUNC_ERROR_HANDLE:  Replacing filter ddoc for {0}.  {1} retries remaining.'.format(source, retries))
				create_filter_func(source, auth, response['_rev'], retries)
		else:
			logging.info('FILTER_FUNC_ERROR_HANDLE:  User elected to skip conflicted filter ddoc for {0}.'.format(source))
			print 'Skipping filtered replication for {0}!'.format(source)

	# There was no conflict, but it still failed.
	else:
		num_failed_ddocs += 1
		print 'Failed creating a filtered replication ddoc for {0}!'.format(source)
		print json.dumps(response, indent=4)
		raise ResponseError('FILTER_FUNC_ERROR_HANDLE: Failed filter ddoc post for {0}!'.format(source), logging.ERROR, r)



# Accepts:	1) The destination Cloudant account URL,
#			2) The name of the DB to create,
#			3) The authorization header for the destination
# Returns:	Void
def create_new_db(url, db, auth):
	global num_failed_repl

	r = requests.put(url + db + '?q=' + new_q, headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			logging.info('CREATE_NEW_DB: {0} already exists on {1}.  Skipping...'.format(db, url))
			print 'Database {0} already exists, continuing...'.format(db)
		else:
			num_failed_repl += 1
			print 'Failed to create database {0}!'.format(db)
			raise ResponseError('CREATE_NEW_DB: Failed to create {0} on {1}!'.format(db, url), logging.ERROR, r)
	else:
		print 'Successfully created database {0}.'.format(db)



# Accepts:	1) The source Cloudant account URL,
#			2) The replication document to post,
#			3) The authorization header for the source
# Returns:	Void
def post_repl_doc(url, doc, auth):
	global num_failed_repl

	source = doc['source']['url']

	# post the doc
	r = requests.post(url + '_replicator', data=json.dumps(doc), 
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# check for errors
	if 'error' in r:
		num_failed_repl += 1
		print 'Failed to post a replication doc for {0}!'.format(source)
		print json.dumps(r, indent=4)
		raise ResponseError('POST_REPL_DOC: Failed to post a replication doc for {0}!'.format(source), logging.ERROR, r)



def confirm_triggered(url, auth, doc_id, retries=5):
	global num_failed_repl

	# base case - we've run out of retries
	if retries == 0:
		num_failed_repl += 1
		print 'Retry limit (5) exceeded for triggering replication doc "{0}".  Skipping...'.format(doc_id)
		raise GeneralError('CONFIRM_TRIGGERED: Retry limit (5) exceeded for triggering replication doc "{0}".  No replication posted!'.format(doc_id), logging.ERROR)

	repl_doc_url = '{0}_replicator/{1}'.format(url, doc_id)
	r = requests.get(repl_doc_url, headers={'Authorization': auth}).json()

	if 'error' in r:
		print 'Failed to retrieve replication doc "{0}".  Retrying...'.format(doc_id)
		raise ResponseError('CONFIRM_TRIGGERED: Failed to retrieve replication doc "{0}".  Retrying...'.format(doc_id), logging.ERROR, r)

	source = r['source']['url']
	status = r['_replication_state']
	if status == 'triggered' or status == 'completed':
		logging.info('CONFIRM_TRIGGERED: Replication for {0} has triggered or completed.'.format(source))
	else:
		print 'Replication for "{0}" has not triggered.  Rechecking...'.format(source)
		retries -= 1
		logging.info('POST_REPL_DOC: Replication "{0}" has not triggered.  {1} retries remaining.'.format(source, retries))
		post_repl_doc(url, r, auth)
		time.sleep(retry_delay)
		confirm_triggered(url, auth, doc_id, retries)



# Accepts:  1) The URL for the Cloudant account
#			2) The Authorization header
#			3) The number of retries remaining (defaults to 5)
# Returns:  The number of replication tasks that have pending changes
def poll_active_repl(url, auth, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeded.  Failed to retrieve _active_tasks.  Exiting...'
		raise FatalError('POLL_ACTIVE_REPL: Failed to retreive _active_tasks for {0}!  Retries exceeded!'.format(url), logging.CRITICAL)

	num_repl = 0
	tasks = requests.get(url + '_active_tasks', headers={'Authorization': auth}).json()

	# handle potential errors
	if 'error' in tasks:
		print 'Failed to retrieve _active_tasks.  Retrying...'
		retries -= 1
		logging.warning('POLL_ACTIVE_REPL: Failed to retrieve _active_tasks for {0}.  {1} retries remaining.'.format(url, retries))
		logging.warning(json.dumps(tasks, indent=4))
		return poll_active_repl(url, auth, retries)

	# iterate over each doc returned
	for doc in tasks:
		if doc['type'] == 'replication' and doc['changes_pending'] > 0:
			num_repl += 1
	return num_repl



# Accepts:	1) The URL for the Cloudant account
#			2) The authorization header for the account
# Returns:	Void
def create_replicator(url, auth, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeeded.  Failed to create database _replicator.  Exiting...'
		raise FatalError('CREATE_REPLICATOR: Failed to create database _replicator!  Exiting...', logging.CRITICAL)

	r = requests.put('{0}_replicator'.format(url), headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			print 'Database _replicator already exists, continuing...'
		else:
			print 'Failed to create database _replicator!  Retrying...'
			retries -= 1
			logging.warning('CREATE_REPLICATOR: Failed to create database _replicator for {0}.  {1} retries remaining.'.format(url, retries))
			logging.warning(json.dumps(r, indent=4))
			create_replicator(url, auth, retries)
	else:
		print 'Successfully created database _replicator.'



def main():
	logging.info('\n\n#################################################################')
	# locate all dbs for account1
	print 'Reading dbs requested for replication...'
	dbs = get_dbs(url1, auth1)
	print 'Retrieved {0} dbs.  Beginning the replication process...'.format(len(dbs))

	# create the _replicator db on the source if it doesn't already exist
	create_replicator(url1, auth1)

	# iterate through array of dbs and initiate replications to targets
	replications = []
	db_index = 0
	while db_index < len(dbs):
		try:
			# we only spawn new replications if it's below the limit.  Don't want
			# to overload the cluster. 
			if poll_active_repl(url1, auth1) < concurrency_limit:
				# db1 -> source db, db2 -> target db
				db1 = db2 = dbs[db_index]

				# make the target db name unique if required
				if rename_dbs:
					db2 += '2'

				# attempt to create the target db with the new q value if desired
				if not use_default_q:
					create_new_db(url2, db2, auth2)

				# build a replication doc
				doc = create_repl_doc(url1 + db1, auth1, url2 + db2, auth2, repl_options)

				# create a design document for filtering ddocs if desired
				if skip_ddocs:
					create_filter_func(url1 + db1, auth1)
					doc.update({'filter': '{0}/{1}'.format(skip_ddocs_name, skip_ddocs_func)})

				# post the doc to the source db (i.e. the mediator)
				post_repl_doc(url1, doc, auth1)
				print 'DB replication for {0} initiated...'.format(dbs[db_index])
				replications.append(doc['_id'])

				# increment index in to array of dbs
				db_index += 1

			else:
				# sleep for an arbitrary amount of time before polling again
				print 'Concurrent replication limit reached...waiting...'
				time.sleep(10)

		except GeneralError as e:
			logging.log(e.level, e.msg)
			db_index += 1 
		except ResponseError as re:
			logging.log(re.level, re.msg)
			logging.log(re.level, json.dumps(re.r, indent=4))
			db_index += 1
		except FatalError as fe:
			logging.log(fe.level, fe.msg)
			exit()

	# if we're forcing a triggered state, we verify each
	if force_triggered:
		print '\n\nReplication docs posting complete.'
		print 'Verifying each replication has triggered...'
		print 'View the log for details...'

		repl_index = 0
		while repl_index < len(replications):
			try:
				confirm_triggered(url1, auth1, replications[repl_index])
			except GeneralError as e:
				logging.log(e.level, e.msg)
			except ResponseError as re:
				logging.log(re.level, re.msg)
				logging.log(re.level, json.dumps(re.r, indent=4))
			finally:
				repl_index += 1


	# we're done replicating the list of databases
	print '\n#####################'
	print 'All databases processed for replication!'
	print 'Failed replications:  {0}'.format(num_failed_repl)
	print 'Failed filter ddocs:  {0}'.format(num_failed_ddocs)
	logging.info('\n#####################')
	logging.info('All databases processed for replication!')
	logging.info('Failed replications:  {0}'.format(num_failed_repl))
	logging.info('Failed filter ddocs:  {0}'.format(num_failed_ddocs))


if __name__ == "__main__":
	main()