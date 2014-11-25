# Author:  Ryan Millay, SE - Cloudant
# This file defines all of the logic used for spawning replications and confirming
# they have triggered or completed.

import requests
import json
import logging
from ExceptionsModule import ReplError
from ExceptionsModule import FatalError
from ExceptionsModule import FilterError


s = requests.Session()

# Accepts:  1) A URL to a cloudant account, 
#           2) the authorization header
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
#           2) The source authorization header,
#           3) The target URL (both Cloudant account and db),
#           4) The target authorization header,
#           5) The account to be listed in the user_ctx,
#           6) Additional replication options,
#           7) Unique id to use with this set of replications
# Returns:  The prepared replication document
def create_repl_doc(source, source_auth, target, target_auth, mediator, repl_options, batch_id, incremental_id):
	db = source.split('cloudant.com/')[1]
	base_url = source.split(db)[0]
	doc_id = 'cloudant_bulk_replication_{0}_{1}'.format(db, batch_id)

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
			'_id': doc_id,
			'create_target': True,
			'batch_id': batch_id	
	}

	if incremental_id is not None:
		seq_num = get_last_sequence_num(base_url, source_auth, db, incremental_id)
		doc.update({'since_seq': seq_num})

	# add the replication paramaters
	doc.update(repl_options)

	return doc



# Accepts:  1) The destination Cloudant account URL,
#           2) The name of the DB to create,
#           3) The authorization header for the destination,
#           4) The new q value for the database
# Returns:  Void
def create_new_db(url, db, auth, q):
	r = s.put(url + db + '?q=' + q, headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			logging.info('CREATE_NEW_DB: {0} already exists on {1}.  Skipping...'.format(db, url))
			print 'Database {0} already exists, continuing...'.format(db)
		else:
			print 'Failed to create database {0}!\n{1}'.format(db, json.dumps(r, indent=4))
			raise ReplError('CREATE_NEW_DB: Failed to create {0} on {1}!'.format(db, url), logging.ERROR, r)



# Accepts:  1) The mediator Cloudant account URL,
#           2) The replication document to post,
#           3) The authorization header for the mediator
# Returns:  Void
def post_repl_doc(url, doc, auth):
	source = doc['source']['url']

	# post the doc
	r = s.post(url + '_replicator', data=json.dumps(doc), 
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# check for errors
	if 'error' in r:
		print 'Failed to post a replication doc for {0}!\n{1}'.format(source, json.dumps(r, indent=4))
		raise ReplError('POST_REPL_DOC: Failed to post a replication doc for {0}!'.format(source), logging.ERROR, r)



# Accepts:  1) The URL for the Cloudant account
#           2) The authorization header for the account
# Returns:  Void
def create_replicator(url, auth):
	r = s.put('{0}_replicator'.format(url), headers={'Authorization': auth}).json()
	if 'error' in r:
		if r['error'] == 'file_exists':
			print 'Database _replicator already exists, continuing...'
		else:
			print 'Failed to create database _replicator!'
			raise FatalError('CREATE_REPLICATOR: Failed to create _replicator for {0}.'.format(url), logging.ERROR, r)
	else:
		print 'Successfully created database _replicator.'



# Accepts:  1) The URL for the Cloudant account,
#           2) The authorization header for the account,
#           3) The source db that is slated to be replicated,
#           4) The last batch id used for replications
# Returns:  The last sequence number to include in the replication doc
def get_last_sequence_num(url, auth, source_db, batch_id):
	# first get the replication id
	doc_url = '{0}_replicator/cloudant_bulk_replication_{1}_{2}'.format(url, source_db, batch_id)
	r = s.get(doc_url, headers={'Authorization': auth}).json()

	if 'error' in r or '_replication_id' not in r:
		print 'Failed to find sequence number for incremental replication of {0}!'.format(source_db)
		raise ReplError('GET_LAST_SEQUENCE_NUM: Failed to find sequence number for incremental replication of {0}!'.format(source_db), logging.ERROR, r)

	repl_id = r['_replication_id']

	# next get the sequence number
	local_url = '{0}{1}/_local/{2}'.format(url, source_db, repl_id)
	r = s.get(local_url, headers={'Authorization': auth}).json()

	if 'error' in r and r['error'] == 'not_found':
		print 'Previous seq number for {0} for batch {1} not available.  Trying next most recent batch ID...'.format(source_db, batch_id)
		logging.warning('GET_LAST_SEQUENCE_NUM: No previous sequence number for {0} was found for batch ID {1}.'.format(source_db, batch_id))
		logging.warning('GET_LAST_SEQUENCE_NUM: This is ok.  Previous replication for this db likely did not replicate any docs.')
		return try_other_batch_ids(url, auth, source_db, batch_id)
	elif 'error' in r:
		print 'Failed to find sequence number for incremental replication of {0}!'.format(source_db)
		raise ReplError('GET_LAST_SEQUENCE_NUM: Failed to find sequence number for incremental replication of {0}!'.format(source_db), logging.ERROR, r)
	else:
		seq_num = r['history'][0]['recorded_seq']
		logging.info('GET_LAST_SEQUENCE_NUM: database: {0}, previous batch id: {1}, last sequence number: {2}'.format(source_db, batch_id, seq_num))
		return seq_num



# Accepts:  1) The URL for the Cloudant account,
#           2) The authorization header for the account,
#           3) The source db that is slated to be replicated,
#           4) The previous batch ID which did not lead to a sequence number
# Returns:  The last sequence number for the next most recent batch ID
def try_other_batch_ids(url, auth, source_db, failed_batch_id):
	# Query the _replicator for all docs that have the "cloudant_bulk_replication_<source_db>" base.
	# Set the end key to the replication that resulted in a missing sequence number and disable inclusive_end.
	# The results will be sorted based on batch ID (i.e. time) in ascending order.
	startkey = 'cloudant_bulk_replication_{0}'.format(source_db)
	endkey = '{0}_{1}'.format(startkey, failed_batch_id)
	full_url = '{0}_replicator/_all_docs?startkey="{1}"&endkey="{2}"&inclusive_end=false&include_docs=true'.format(url, startkey, endkey)
	r = s.get(full_url, headers={'Authorization': auth}).json()

	if 'error' in r or 'rows' not in r or len(r['rows']) == 0:
		print 'Unable to find a useable sequence number for {0}!'.format(source_db)
		raise FatalError('TRY_OTHER_BATCH_IDS: Unable to find a useable sequence number for {0}!'.format(source_db), logging.ERROR, r)

	num_results = len(r['rows'])
	next_repl_batch = r['rows'][num_results - 1]['doc']['batch_id']
	return get_last_sequence_num(url, auth, source_db, next_repl_batch)
