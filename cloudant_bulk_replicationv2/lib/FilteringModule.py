# Author:  Ryan Millay, SE - Cloudant
# This file contains logic for deploying a ddoc with a filter to skip ddocs during replication.

import json
import requests
import logging
import uuid
from ExceptionsModule import FilterError

s = requests.Session()
name = 'filteringddoc'
func = 'filteringfunc'

# Accepts:  1) The source URL (both the Cloudant account and db),
#           2) The authorization header
# Returns:  Json object (Dictionary) representing the ddoc name and func name
def create_filter_func(source, auth):

	ddoc = {
			'_id': '_design/{0}'.format(name),
			'filters': {
						'{0}'.format(func): 
							'function(doc, req) {if(doc._id.indexOf("_design") == 0) {return false;} else {return true;}}'
			}
	}

	# post the doc to cloudant
	r = s.post(source, data=json.dumps(ddoc),
			headers={'content-type': 'application/json', 'Authorization': auth}).json()

	# handle conflicts
	if 'error' in r and r['error'] == 'conflict':
		print 'The ddoc responsible for filtering ddocs already exists.  Skipping...'
	elif 'error' in r:
		print 'Failed creating a filtered replication ddoc for {0}!\n{1}'.format(source, json.dumps(response, indent=4))
		raise FilterError('CREATE_FILTER_FUNC: Failed filter ddoc post for {0}!'.format(source), logging.ERROR, r)

	return {'name': name, 'func': func}



# Accepts:  1) The source URL (both the Cloudant account and db),
#           2) The authorization header
# Returns:  Void
def remove_filter_func(source, auth):
	# first read back the doc
	url = '{0}/_design/{1}'.format(source, name)
	r = s.get(url, headers={'Authorization': auth}).json()

	if 'error' in r or '_rev' not in r:
		print 'Failed to delete filtering ddoc for {0}'.format(source)
		raise FilterError('REMOVE_FILTER_FUNC: Failed to delete filtering ddoc for {0}!'.format(source), logging.ERROR, r)

	url += '?rev={0}'.format(r['_rev'])
	r = s.delete(url, headers={'Authorization': auth}).json()

	if 'error' in r:
		print 'Failed to delete filtering ddoc for {0}'.format(source)
		raise FilterError('REMOVE_FILTER_FUNC: Failed to delete filtering ddoc for {0}!'.format(source), logging.ERROR, r)