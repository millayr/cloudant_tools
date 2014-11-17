# Author:  Ryan Millay, SE - Cloudant
# This file contains logic for deploying a ddoc with a filter to skip ddocs during replication.

import json
import requests
import logging
import uuid
from ExceptionsModule import ReplError
from ExceptionsModule import FilterError
from ExceptionsModule import FilterError

s = requests.Session()

# Accepts:  1) The source URL (both the Cloudant account and db),
#			2) The authorization header
# Returns:  Json object (Dictionary) representing the ddoc name and func name
def create_filter_func(source, auth):
	name = uuid.uuid1()
	func = uuid.uuid1()

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

	# Handle potential errors
	if 'error' in r:
		print 'Failed creating a filtered replication ddoc for {0}!\n{1}'.format(source, json.dumps(response, indent=4))
		raise FilterError('FILTER_FUNC_ERROR_HANDLE: Failed filter ddoc post for {0}!'.format(source), logging.ERROR, r)

	return {'name': name, 'func': func}
