import requests
import json
import multiprocessing.dummy as multiprocessing
import time
import os
import sys
import getopt
import getpass


# configuration values
config = dict(
    accountname = '',
    username = '',
    password = '',
    outputpath = './all_docs_output_{0}'.format(int(time.time())),
    authheader = '',
    num_threads = 20,
    )

usage = 'python ' + os.path.basename(__file__) + ' -a <accountname> [-u <username>]'

def parse_args(argv):
    # parse through the argument list and update the config dict as appropriate
    try:
        opts, args = getopt.getopt(argv, "hu:a:", ["help", "username=", "accountname="])
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-u", "--username"):
            config['username'] = arg
        elif opt in ("-a", "--accountname"):
            config['accountname'] = arg


def init_config():
    if config['accountname'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        config['username'] = config['accountname']
    
    config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['accountname'])


def get_password():
    config['password'] = getpass.getpass('Password for {0}:'.format(config["username"]))


def authenticate():
    header = {'Content-type': 'application/x-www-form-urlencoded'}
    url = config['baseurl'] + '_session'
    data = dict(name=config['username'],
                password=config['password'])
    response = requests.post(url, data = data, headers = header)
    if 'error' in response.json():
        if response.json()['error'] == 'forbidden':
            print response.json()['reason']
            sys.exit()
    config['authheader'] = {'Cookie': response.headers['set-cookie']}


def stream_all_docs(queue):
	s = requests.Session()
	while True:
		db = queue.get()
		if db is None:
			print 'End of database list reached.  Thread exiting...'
			break

		r = s.get('{0}{1}/_all_docs?include_docs=true'.format(config['baseurl'], db), headers=config['authheader'], stream=True)
			
		with open("{0}/{1}.json".format(config['outputpath'], db), 'wb') as f:
			for chunk in r.iter_content(chunk_size=5000000):
				if chunk:
					f.write(chunk)
					f.flush()

		print 'Saved {0}...'.format(db)


def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()

    if not os.path.exists(config['outputpath']):
        os.makedirs(config['outputpath'])

    dbs = requests.get('{0}_all_dbs'.format(config['baseurl']), headers=config['authheader']).json()
    if '_replicator' in dbs:
        dbs.remove('_replicator')

    q = multiprocessing.Queue()
    for db in dbs:
        q.put(db)

    threads = []
    for i in range(config['num_threads']):
        t = multiprocessing.Process(target=stream_all_docs, args=(q,))
        threads.append(t)
        t.start()
        q.put(None)

    for t in threads:
        t.join()

if __name__ == "__main__":
	main(sys.argv[1:])
