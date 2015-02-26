import os
import sys
import getopt
import getpass
import base64
import json
import requests
import fileinput
import multiprocessing.dummy as multiprocessing

from pprint import pprint

# configuration values
config = dict(
    accountname = '',
    username = '',
    password = '',
    inputpath = '',
    #the number of rows to upload per bulk operation
    blocksize = 500,
    authheader = '',
    num_threads = 20,
    restore = False
    )
usage = 'python ' + os.path.basename(__file__) + ' -p <path to json file or dir> -a <accountname> [-u <username>] [-b <# of records/update] [-r]'


def parse_args(argv):
    # parse through the argument list and update the config dict as appropriate
    try:
        opts, args = getopt.getopt(argv, "hp:b:u:a:r", 
                                   ["help",
                                    "path=",
                                    "blocksize=",
                                    "username=",
                                    "accountname=",
                                    "restore"
                                    ])
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-p", "--path"):
            config['inputpath'] = arg
        elif opt in ("-b", "--blocksize"):
            config['blocksize'] = int(arg)
        elif opt in ("-u", "--username"):
            config['username'] = arg
        elif opt in ("-a", "--accountname"):
            config['accountname'] = arg
        elif opt in ("-r", "restore"):
            config['restore'] = True
 

def init_config():
    if config['inputpath'] == '':
        print usage
        sys.exit()
    if config['accountname'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        config['username'] = config['accountname']
    
    config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['accountname'])

    # lets change the working directory to make things easier later on
    if os.path.isfile(config['inputpath']):
        last_index_of_slash = config['inputpath'].rfind('/')
        if last_index_of_slash != -1:
            os.chdir(config['inputpath'][:last_index_of_slash+1])
            config['inputpath'] = config['inputpath'][last_index_of_slash+1:]
    else:
        os.chdir(config['inputpath'])

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


def initialize_db(dbname, session):
    r = session.put('{0}{1}'.format(config['baseurl'], dbname), headers = config['authheader'])
    if r.status_code != 201:
        print 'The database "{0}" was not created!'.format(dbname)
        sys.exit()


def delete_db(dbname, session):
    r = session.delete('{0}{1}'.format(config['baseurl'], dbname), headers = config['authheader'])
    if r.status_code != 200:
        print 'Failed to delete database "{0}"!'.format(dbname)


def updatedb(dbname, requestdata, session):
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = session.post(
        '{0}{1}/_bulk_docs'.format(config['baseurl'], dbname),
        headers = headers,
        data = json.dumps(requestdata)
        )
    if len(r.json()) > 0:
        print json.dumps(r.json(), indent=4)


def upload(filename, dbname, session):
    blockcounter = 0
    rowcounter = 0
    requestdata = dict(new_edits=False,docs=[])
    for line in fileinput.FileInput(filename):
        try:
            line = line.rstrip()
            if line[-1] == ',':
                line = line[:-1]

            bloated_doc = json.loads(line)

            if blockcounter >= config['blocksize']:
                #update db
                updatedb(dbname, requestdata, session)
                #reset the temp dict and counter
                requestdata = dict(new_edits=False,docs=[])
                blockcounter = 0

            #add row to temp dict
            requestdata['docs'].append(bloated_doc['doc'])
            #increment the row counter
            blockcounter += 1        
        except:
            if rowcounter != 0 and line != ']}':
                print 'An exception occured on line {0}'.format(rowcounter)
        finally:
            rowcounter += 1
    fileinput.close()
        
    #write any remaining rows to the database
    updatedb(dbname, requestdata, session)

    print 'Database "{0}" uploading completed.'.format(dbname)


def upload_dispatcher(queue):
    s = requests.Session()
    while True:
        f = queue.get()
        if f is None:
            print 'End of restore list reached.  Thread exiting...'
            break

        dbname = f.split('.')[0]

        if config['restore'] == True:
            delete_db(dbname, s)

        initialize_db(dbname, s)
        upload(f, dbname, s)


def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()

    q = multiprocessing.Queue()
    # if we're processing a directory which contains X number of json files
    if os.path.isfile(config['inputpath']):
        q.put(config['inputpath'])
        config['num_threads'] = 1
    else:
        for f in os.listdir('.'):
            q.put(f)

    threads = []

    for i in range(config['num_threads']):
        t = multiprocessing.Process(target=upload_dispatcher, args=(q,))
        t.setDaemon(True)
        threads.append(t)
        t.start()
        q.put(None)

    for t in threads:
        t.join()



if __name__ == "__main__":
    main(sys.argv[1:])
