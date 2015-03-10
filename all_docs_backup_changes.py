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
    checkpointpath = '.db_changes_checkpoint',
    tmpcheckpointpath = '.db_changes_checkpoint.tmp',
    authheader = '',
    num_threads = 20,
    baseurl = ''
    )

usage = 'python ' + os.path.basename(__file__) + ' -a <accountname> [-u <username>] [-b <base url>]'

def parse_args(argv):
    # parse through the argument list and update the config dict as appropriate
    try:
        opts, args = getopt.getopt(argv, "hu:a:b:", ["help", "username=", "accountname=", "url="])
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
        elif opt in ("-b", "--url"):
            config['baseurl'] = arg


def init_config():
    if config['accountname'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        config['username'] = config['accountname']
    # if no URL was specified, assumed this is DBaaS
    if config['baseurl'] == '':
        config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['accountname'])
    # set the path to the checkpoint file
    if os.path.dirname(__file__) != '':
        config['checkpointpath'] = '{0}/.db_changes_checkpoint'.format(os.path.dirname(__file__))
        config['tmpcheckpointpath'] = '{0}/.db_changes_checkpoint.tmp'.format(os.path.dirname(__file__))


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
            break

        r = s.get('{0}{1}/_changes?include_docs=true'.format(config['baseurl'], db), headers=config['authheader'], stream=True)

        with open("{0}/{1}.json".format(config['outputpath'], db), 'wb') as f:
			for chunk in r.iter_content(chunk_size=5000000):
				if chunk:
					f.write(chunk)
					f.flush()

        print 'Saved {0}...'.format(db)


def write_checkpoint(db_changes):
    checkpoint_file = open(config['tmpcheckpointpath'], 'w')
    checkpoint_file.write(json.dumps(db_changes['last_seq']))
    checkpoint_file.close()


def get_changed_dbs():
    # if a checkpoint exists, run _db_updates with since parameter, otherwise just run _db_updates
    if os.path.exists(config['checkpointpath']):
        # print "the checkpoint file exists!!!"
        f = open(config['checkpointpath'], 'r')
        checkpoint_seq = f.read()
        db_changes = requests.get('{0}_db_updates?since={1}'.format(config['baseurl'], checkpoint_seq), headers=config['authheader']).json()
    else:
        db_changes = requests.get('{0}_db_updates'.format(config['baseurl']), headers=config['authheader']).json()

    return db_changes


def remove_tmp_checkpoint():
    if os.path.exists(config['tmpcheckpointpath']):
        os.remove(config['tmpcheckpointpath'])


def rename_checkpoint_file():
    os.rename(config['tmpcheckpointpath'], config['checkpointpath'])


def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()
    remove_tmp_checkpoint()

    if not os.path.exists(config['outputpath']):
        os.makedirs(config['outputpath'])

    # get updated databases
    db_changes = get_changed_dbs()

    # save the latest checkpoint to .db_changes_checkpoint file prior to processing
    write_checkpoint(db_changes)

    q = multiprocessing.Queue()
    unique = set()
    for db_object in db_changes['results']:
        db = db_object['dbname']
        if db not in ['_replicator','metrics','dbs'] and db not in unique:
            unique.add(db)
            q.put(db)

    threads = []
    for i in range(config['num_threads']):
        t = multiprocessing.Process(target=stream_all_docs, args=(q,))
        threads.append(t)
        t.start()
        q.put(None)

    for t in threads:
        t.join()

    rename_checkpoint_file()

if __name__ == "__main__":
	main(sys.argv[1:])
