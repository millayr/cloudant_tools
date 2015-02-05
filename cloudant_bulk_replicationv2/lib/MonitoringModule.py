# Author:  Ryan Millay, SE - Cloudant
# This file contains logic to deploy a ddoc to the _replicator db and to poll for the
# number of replications currently running.

import json
import requests
import logging
import time
from ExceptionsModule import FatalError

s = requests.Session()



# Accepts:  1) The base URL to the mediator account,
#           2) The authorization for the mediator
# Returns:  The URL to the newly created view
def create_repl_index(url, auth):
	name = 'num_running_repl'
	function =  'function(doc) {if(doc._id.indexOf("cloudant_bulk_replication") === 0 '
	function += '&& (!doc._replication_state || (doc._replication_state && doc._replication_state == "triggered"))) '
	function += '{emit(doc._id, 1);}}'

	ddoc = {
		'_id': '_design/{0}'.format(name),
		'views': {
			'replRunning': {
				'map': function,
				'reduce': '_count'
			}
		}
	}

	# post the total ddoc
	r = s.post('{0}_replicator'.format(url), data=json.dumps(ddoc), 
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	if 'error' in r and r['error'] == 'conflict':
		print 'The ddoc responsible for monitoring replications already exists.  Skipping...'
	elif 'error' in r:
		print 'Failed posting a ddoc to {0}_replicator!\n{1}'.format(url, json.dumps(r, indent=4))
		raise FatalError('CREATE_REPLICATION_INDEX: Failed posting a ddoc to {0}_replicator!'.format(url), logging.ERROR, r)

	return '{0}_replicator/_design/{1}/_view/replRunning'.format(url, name)



# Accepts:  1) The view URL to monitor on the mediator,
#           2) The authorization for the mediator,
#           3) The limit for concurrent replications,
#           4) Number of retries remaining (defaults to 5)
# Returns:  True if more replications can be POSTed, False otherwise
def poll_replicator(url, auth, limit, retries=5):
	# base case - we've run out of retries
	if retries == 0:
		print 'Retry limit (5) exceeded.  Failed to retrieve {0}.  Exiting...'.format(url)
		raise FatalError('POLL_REPLICATOR: Failed to retreive {0}!  Retries exceeded!'.format(url), logging.CRITICAL)

	r = s.get(url, headers={'Authorization': auth}).json()

	# handle potential errors
	if 'error' in r:
		print 'Failed to retrieve {0}.  Retrying...'.format(url)
		retries -= 1
		logging.warning('POLL_REPLICATOR: Failed to retrieve {0}.  {1} retries remaining.\n{2}'.format(url, retries, json.dumps(r, indent=4)))
		time.sleep(5)
		return poll_replicator(url, auth, limit, retries)

	repl_running =  0
	if 'rows' in r and len(r['rows']) == 1:
		repl_running = r['rows'][0]['value']

	if repl_running >= limit:
		logging.info('POLL_REPLICATOR: Max concurrent replications reached ({0}).  Waiting.'.format(repl_running))
		return False
	else:
		logging.info('POLL_REPLICATOR: There are {0} replications running.  Additional replications may be POSTed.'.format(repl_running))
		return True