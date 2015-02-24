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
    inputfile = '',
    inputdir = '',
    #the number of rows to upload per bulk operation
    blocksize = 500,
    authheader = '',
    num_threads = 20
    )
usage = 'python ' + os.path.basename(__file__) + ' -f <json file to restore> -a <accountname> [-u <username>] [-b <# of records/update] [-d <dir path>]'


def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    try:
        opts, args = getopt.getopt(argv, "hf:b:u:a:d:", 
                                   ["help",
                                    "file=",
                                    "blocksize=",
                                    "username=",
                                    "accountname=",
                                    "dir="
                                    ])
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-f", "--file"):
            config['inputfile'] = arg
        elif opt in ("-b", "--blocksize"):
            config['blocksize'] = int(arg)
        elif opt in ("-u", "--username"):
            config['username'] = arg
        elif opt in ("-a", "--accountname"):
            config['accountname'] = arg
        elif opt in ("-d", "--dir"):
            config['inputdir'] = arg
 

def init_config():
    if config['inputfile'] == '' and config['inputdir'] == '':
        print usage
        sys.exit()
    if config['accountname'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        config['username'] = config['accountname']
    
    config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['accountname'])

    # lets change the working directory to make things easier later on
    if config['inputdir'] != '':
        os.chdir(config['inputdir'])
    else:
        last_index_of_slash = config['inputfile'].rfind('/')
        if last_index_of_slash != -1:
            os.chdir(config['inputfile'][:last_index_of_slash+1])
            config['inputfile'] = config['inputfile'][last_index_of_slash+1:]


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


def initialize_db(dbname):
    r = requests.put('{0}{1}'.format(config['baseurl'], dbname), headers = config['authheader'])
    if r.status_code != 201:
        print 'The database "{0}" was not created!'.format(dbname)
        sys.exit()


def delete_db(dbname):
    r = requests.delete('{0}{1}'.format(config['baseurl'], dbname), headers = config['authheader'])
    if r.status_code != 200:
        print 'Failed to delete database "{0}"!'.format(dbname)


def updatedb(dbname, requestdata):
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = requests.post(
        '{0}{1}/_bulk_docs'.format(config['baseurl'], dbname),
        headers = headers,
        data = json.dumps(requestdata)
        )
    if len(r.json()) > 0:
        print json.dumps(r.json(), indent=4)
    else:
        print 'Bulk request complete for database "{0}"...'.format(dbname)


def upload(filename, dbname):
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
                updatedb(dbname, requestdata)
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
        
    #write any remaining rows to the database
    updatedb(dbname, requestdata)


def slice_array(array, size):
    if size < 1:
        size = 1
    return [array[i:i + size] for i in range(0, len(array), size)]


def upload_dispatcher(file_group):
    for f in file_group:
        dbname = f.split('.')[0]
        delete_db(dbname)
        initialize_db(dbname)
        upload(f, dbname)


def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()

    # if we're processing a directory which contains X number of json files
    if config['inputdir'] != '':
        files = os.listdir('.')
        file_groups = slice_array(files, len(files) // config['num_threads'])
        threads = []

        for group in file_groups:
            t = multiprocessing.Process(target=upload_dispatcher, args=(group,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
    # otherwise we're just restoring a single file
    else:
        upload_dispatcher([config['inputfile']])


if __name__ == "__main__":
    main(sys.argv[1:])
