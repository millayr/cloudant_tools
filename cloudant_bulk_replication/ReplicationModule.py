# Author:  Ryan Millay, SE - Cloudant
# This file defines all of the logic used for spawning replications and confirming
# they have triggered or completed.

import requests
import json
import time
import random
import logging
import uuid
from ExceptionsModule import ReplError
from ExceptionsModule import FatalError
from ExceptionsModule import FilterError


s = requests.Session()

# Accepts:  1) A URL to a cloudant account, 
#			2) the authorization header
# Returns:  An array of all databases slated for replication from account1 to account2
def get_dbs(url, auth):
	r = s.get('{0}_all_dbs'.format(url), headers={'Authorization': auth}).json()
	if '_replicator' in r:
		r.remove('_replicator')
	if '_warehouser' in r:
		r.remove('_warehouser')

	logging.info('GET_DBS: {0} databases slated for replication:'.format(len(r)))
	logging.info(json.dumps(r, indent=4))

	return r



# Accepts:  1) The source URL (both Cloudant account and db),
#			2) The source authorization header,
#			3) The target URL (both Cloudant account and db),
#			4) The target authorization header,
#			5) The account to be listed in the user_ctx
#			6) Any additional replication options (defaults to empty)
# Returns:  The prepared replication document
def create_repl_doc(source, source_auth, target, target_auth, mediator, repl_options={}):
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
				'name': mediator,
				'roles': ['_admin', '_reader', '_writer']
			},
			'_id': 'cloudant_bulk_replication_{0}'.format(name),
			'worker_processes': 2,
			'worker_batch_size': 500,
			'http_connections': 2,
			'connection_timeout': 60000
	}

	# add the replication paramaters
	doc.update(repl_options)

	return doc



# Accepts:	1) The destination Cloudant account URL,
#			2) The name of the DB to create,
#			3) The authorization header for the destination,
#			4) The new q value for the database
# Returns:	Void
def create_new_db(url, db, auth, q):
	r = s.put(url + db + '?q=' + q, headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			logging.info('CREATE_NEW_DB: {0} already exists on {1}.  Skipping...'.format(db, url))
			print 'Database {0} already exists, continuing...'.format(db)
		else:
			print 'Failed to create database {0}!\n{1}'.format(db, json.dumps(r, indent=4))
			raise ReplError('CREATE_NEW_DB: Failed to create {0} on {1}!'.format(db, url), logging.ERROR, r)
	else:
		print 'Successfully created database {0}.'.format(db)



# Accepts:	1) The mediator Cloudant account URL,
#			2) The replication document to post,
#			3) The authorization header for the mediator
# Returns:	Void
def post_repl_doc(url, doc, auth):
	source = doc['source']['url']

	# post the doc
	r = s.post(url + '_replicator', data=json.dumps(doc), 
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# check for errors
	if 'error' in r:
		print 'Failed to post a replication doc for {0}!\n{1}'.format(source, json.dumps(r, indent=4))
		raise ReplError('POST_REPL_DOC: Failed to post a replication doc for {0}!'.format(source), logging.ERROR, r)
	else:
		print '[INITIATED] Replication for {0} has been POSTed...'.format(source)



# Accepts:  1) The URL for the Cloudant account
#			2) The Authorization header
#			3) The limit of replications with pending changes
#			4) The number of retries remaining (defaults to 5)
# Returns:  The number of replication tasks that have pending changes
def poll_active_repl(url, auth, limit, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeded.  Failed to retrieve _active_tasks.  Exiting...'
		raise FatalError('POLL_ACTIVE_REPL: Failed to retreive _active_tasks for {0}!  Retries exceeded!'.format(url), logging.CRITICAL)

	num_repl = 0
	tasks = s.get(url + '_active_tasks', headers={'Authorization': auth}).json()

	# handle potential errors
	if 'error' in tasks:
		print 'Failed to retrieve _active_tasks.  Retrying...'
		retries -= 1
		logging.warning('POLL_ACTIVE_REPL: Failed to retrieve _active_tasks for {0}.  {1} retries remaining.\n{2}'.format(url, retries, json.dumps(tasks, indent=4)))
		time.sleep(random.randint(1, 4))
		return poll_active_repl(url, auth, limit, retries)

	# iterate over each doc returned
	for doc in tasks:
		if doc['type'] == 'replication' and doc['changes_pending'] > 0:
			num_repl += 1
			if num_repl >= limit:
				# there are too many replication tasks, return false
				return False

	# limit was not reached, return true to allow for an additional replication to POST
	return True



# Accepts:	1) The URL of the cloudant account that mediates the replication,
#			2) The authorization header,
#			3) The document ID for the given replication,
#			4) Time delay to wait between retries,
#			5) Number of retries before failure
# Returns:	Void
def confirm_triggered(url, auth, doc_id, delay, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeded for triggering replication doc "{0}".  Skipping...'.format(doc_id)
		raise ReplError('CONFIRM_TRIGGERED: Retry limit (5) exceeded for triggering replication doc "{0}".  No replication posted!'.format(doc_id), logging.ERROR)

	repl_doc_url = '{0}_replicator/{1}'.format(url, doc_id)
	r = s.get(repl_doc_url, headers={'Authorization': auth}).json()

	if 'error' in r:
		print 'Failed to retrieve replication doc "{0}"!'.format(doc_id)
		raise ReplError('CONFIRM_TRIGGERED: Failed to retrieve replication doc "{0}".'.format(doc_id), logging.ERROR, r)

	source = r['source']['url']

	if '_replication_state' in r and r['_replication_state'] == 'error':
		print 'Replication for {0} has errors.  Please inspect the logs for more info.'.format(source)
		logging.warning('Replication for {0} has errors.\n{1}'.format(source, json.dumps(r, indent=4)))

	elif '_replication_state' in r and (r['_replication_state'] == 'triggered' or r['_replication_state'] == 'completed'):
		print '[VERIFIED] Replication for {0} has either completed or triggered.'.format(source)
		logging.info('CONFIRM_TRIGGERED: Replication for {0} has triggered or completed.'.format(source))

	else:
		retries -= 1
		logging.info('POST_REPL_DOC: Replication "{0}" has not triggered.  {1} retries remaining.'.format(source, retries))
		time.sleep(delay)
		confirm_triggered(url, auth, doc_id, delay, retries)



# Accepts:	1) The URL for the Cloudant account
#			2) The authorization header for the account
# Returns:	Void
def create_replicator(url, auth):
	r = s.put('{0}_replicator'.format(url), headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			print 'Database _replicator already exists, continuing...'
		else:
			print 'Failed to create database _replicator!'
			logging.warning('CREATE_REPLICATOR: Failed to create _replicator for {0}.\n{1}'.format(url, json.dumps(r, indent=4)))
	else:
		print 'Successfully created database _replicator.'