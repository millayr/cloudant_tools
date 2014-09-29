import requests
from json import dumps
 
### * Create a new database called "sessiondb4" on each cluster via the database API with the query parameter ?q=12
### * Start a continuous replication from sessiondb3 to sessiondb4
### * Start a continuous replication from sessiondb4 to sessiondb3
### * Start a continuous replication from sessiondb4 on us-west to sessiondb4 on us-east
### * Wait for the "changes_pending" field in _active_tasks is at a steady state at or around 0 for at least one hour
### * Push a code change that begins pointing at the new sessiondb4 database. This can be done incrementally as there's
### * a bidirectional replication relationship between sessiondb3 and sessiondb4
### * Confirm that all application servers are pointing at the new database, sessiondb4
### * Confirm that there is no pending replication traffic from sessiondb3 to sessiondb4
### * Delete the sessiondb3 west -> east replication document from the _replicator db
### * Delete the sessiondb3 -> sessiondb4 replication document from the _replicator db
### * Delete the sessiondb4 -> sessiondb3 replication document from the _replicator db
### * Delete sessiondb3
 
def make_url(user, db, _id='', prot='https', auth=''):
	if auth:
		auth = '{0}:{1}@'.format(auth[0], auth[1])
	return '{0}://{1}{2}.cloudant.com/{3}/{4}'.format(prot, auth, user, db, _id).rstrip('/')
 
def create_db(user, db, auth, params={}):
	url = make_url(user, db)
	return requests.put(url, auth=auth, params=params)
 
def create_repl_doc(source, target, _id, repl_options={}, https=True):
	if https:
		prot = 'https'
	else: prot = 'http'
	doc = {'_id' : _id,
			'source': make_url(source['user'], source['db'], prot=prot, auth=source['auth']),
			'target': make_url(target['user'], target['db'], prot=prot, auth=target['auth'])
	}
	doc.update(repl_options)
	return doc
 
def push_doc(user, db, doc, auth):
	url = make_url(user, db, _id=doc.pop('_id'))
	return requests.put(url, auth=auth, data=dumps(doc))
 
def get_all_dbs(user, auth):
	return requests.get('https://{0}.cloudant.com/_all_dbs'.format(user), auth=auth).json()
 
def main():
	auth = {'master' : ('adm-XXXX', 'XXXXXX'),
			'ginprod-east' : ('ginprod-east', 'XXXXXX'),
			'ginprod-west' : ('ginprod-west', 'XXXXXX')
			}
	users = ['ginprod-east', 'ginprod-west']
	new_db = 'sessiondb4'
	old_db = 'sessiondb3'
	repl_options = {'continuous' : True}
	for user in users:
		dbs = get_all_dbs(user, auth[user])
		if '_replicator' not in dbs:
			print create_db(user, '_replicator', auth['master']).url
			print ''
		print create_db(user, new_db, auth[user], params={'q': 64})
		source = {'user' : user,
					'db' : old_db,
					'auth' : auth[user]}
		target = {'user' : user,
					'db' : new_db,
					'auth' : auth[user]}
		_id = '{0}_consistency_rep'.format(new_db)
		doc = create_repl_doc(source, target, _id, repl_options=repl_options, https=False)
		print push_doc(user, '_replicator', doc, auth[user])
		_id = '{0}_consistency_rep'.format(old_db)
		doc = create_repl_doc(target, source, _id, repl_options=repl_options, https=False)
		print push_doc(user, '_replicator', doc, auth[user])
	for user in users:
		o_users = list(set(users) - set([user]))
		print o_users
		for o_user in o_users:
			source = {'user' : user,
						'db' : new_db,
						'auth' : auth[user]}
			target = {'user' : o_user,
						'db' : new_db,
						'auth' : auth[o_user]}
			_id = '{0}_cross_region_consistency_rep'.format(new_db)
			doc = create_repl_doc(source, target, _id, repl_options=repl_options)
			print push_doc(user, '_replicator', doc, auth[user])
 
if __name__ == "__main__":
	main()