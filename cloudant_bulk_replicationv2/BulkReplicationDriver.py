# Author:  Ryan Millay, SE - Cloudant
# Initiate a bulk list of replications between account(s).  Allows for migrating a set of dbs to a new q.

import json
import base64
import logging
import sys
sys.path.insert(0, './lib')
import os
import time
import getopt
import getpass
from ExceptionsModule import ReplError
from ExceptionsModule import FatalError
from ExceptionsModule import FilterError
import ReplicationModule as rm
import FilteringModule as fm
import MonitoringModule as mm

logging.basicConfig(filename='replication.log',level=logging.DEBUG)
start_time = time.strftime('%c')

##### Configuration object #####
config = dict(
	source = '',
	target = '',
	mediator = '',
	rename_dbs = False,
	db_list = [],
	skip_ddocs = False,
	incremental_id = None,
	force_concurrency_limit = True,
	concurrency_limit = 8,
	polling_delay = 5,
	use_default_q = True,
	new_q = '8',
	continuous = False,
	worker_processes = 4,
	worker_batch_size = 500,
	http_connections = 20,
	connection_timeout = 300000,
	source_url = '',
	source_auth = '',
	target_url = '',
	target_auth = '',
	mediator_url = '',
	mediator_auth = ''
)

usage =  'python ' + os.path.basename(__file__) + ' -s <source user> -t <target user> [options]\n\n'
usage += '''\
options:
   -m <mediator>   :   The Cloudant user to mediate the replications.  This 
                       allows for a third party account to perform the 
                       replications.  (Default is to use the source user)

   -d              :   Use this flag to append the date to the end of each
                       database name when it is replicated to the target.  
                       This should be used when the source and target users
                       are the same.  (Default is to not rename the databases)

   -l <filename>   :   Pass a filename containing a list of databases to
                       replicate.  Each database name should be on a separate
                       line.

   -f              :   Use this flag to filter out design docs when
                       replicating.  This will prevent index builds from
                       starting on the target.  (Default is to replicate all
                       design docs)

   -i <#>          :   Create incremental replications based on a previous id.

   -z              :   Use this flag to override the replication concurrency
                       limit.  Note this will impact performance and is not
                       recommended.

   -c <#>          :   Set the concurrency limit for replications.  (Default
                       is 8)

   -p <#>          :   Set the time delay in seconds between polls to 
                       Cloudant when the max concurrent replications have been
                       reached.  (Default is 5)

   -q <#>          :   Set the shard count for all databases replicated to the
                       target.  (Default is to use the cluster default Q)

   -o              :   Make all replications continuous.  Ensure the 
                       concurrency limit is high enough to support all 
                       replications or disable the limit all together.  
                       (Default is to use one-off replications)

   -w <#>          :   Set the number of worker processes per replication.
                       Increase the throughput at the cost of memory and CPU.
                       (Default is 4)

   -b <#>          :   Set the batch size per worker process.  Larger batch 
                       size can offer better performance, while lower values 
                       implies checkpointing is done more frequently.  
                       (Default is 500)

   -C <#>          :   Set the number of http connections per replication.
                       (Default is 20)

   -T <#>          :   Set the timeout for http connections in milliseconds.
                       (Default is 300000)

   -h              :   Display this help message.
'''


# Accepts:  1) An array of dbs to process,
#           2) An output queue for message passing,
#           3) A failure queue for message passing
# Returns:  Void
def repl_dispatcher(dbs, running_repl_url, batch_id):
	db_index = 0
	num_failed_repl = num_failed_ddocs = 0
	db_date = int(time.time())
	replications = []

	repl_options = {
		'continuous': config['continuous'],
		'worker_processes': config['worker_processes'],
		'worker_batch_size': config['worker_batch_size'],
		'http_connections': config['http_connections'],
		'connection_timeout': config['connection_timeout']
	}

	while db_index < len(dbs):
		try:
			# we only spawn new replications if it's below the limit.  Don't want
			# to overload the cluster.
			if not config['force_concurrency_limit'] or mm.poll_replicator(running_repl_url, config['mediator_auth'], config['concurrency_limit']):

				source_db = target_db = dbs[db_index]

				# make the target db name unique if required
				if config['rename_dbs']:
					target_db += '-{0}'.format(db_date)

				# attempt to create the target db with the new q value if desired
				if not config['use_default_q']:
					rm.create_new_db(config['target_url'], target_db, config['target_auth'], config['new_q'])

				# build a replication doc
				repl_source = config['source_url'] + source_db
				repl_target = config['target_url'] + target_db
				doc = rm.create_repl_doc(repl_source, config['source_auth'], repl_target, 
					config['target_auth'], config['mediator'], repl_options, batch_id, config['incremental_id'])

				# create a design document for filtering ddocs if desired
				if config['skip_ddocs']:
					ddoc = fm.create_filter_func(config['source_url'] + source_db, config['source_auth'])
					doc.update({'filter': '{0}/{1}'.format(ddoc['name'], ddoc['func'])})

				# post the doc to the source db (i.e. the mediator)
				rm.post_repl_doc(config['mediator_url'], doc, config['mediator_auth'])
				replications.append(doc['_id'])

				# increment index in to array of dbs
				db_index += 1
				print '[INITIATED] [{0}/{1}] Replication for {2} has been POSTed...'.format(db_index, len(dbs), repl_source)

			else:
				# sleep for an arbitrary amount of time before polling again
				print 'Concurrent replication limit reached...waiting for replications to complete...'
				time.sleep(config['polling_delay'])

		# handle exceptions that may have happened
		except ReplError as re:
			logging.log(re.level, '{0}\n{1}'.format(re.msg, json.dumps(re.r, indent=4)))
			num_failed_repl += 1
			db_index += 1 
		except FilterError as fe:
			logging.log(fe.level, '{0}\n{1}'.format(fe.msg, json.dumps(fe.r, indent=4)))
			num_failed_ddocs += 1
			db_index += 1
		except FatalError as xe:
			logging.log(xe.level, '{0}\nAborting thread operations!!'.format(xe.msg))
			failures.put([dbs, dbs[db_index]])
			sys.exit()
		except:
			print 'Unexpected Error!  View the log for details.'
			logging.error('Unexpected error occured! Error: {0}'.format(sys.exc_info()))
			sys.exit()

	# wait for any remaining replications to finish if not continuous
	if not config['continuous']:
		print '\nWaiting for any remaining replications to complete.\n'
		while not mm.poll_replicator(running_repl_url, config['mediator_auth'], 1):
			time.sleep(config['polling_delay'])

	# delete the filtering ddocs if they were created and if these replications are not continuous
	if config['skip_ddocs'] and not config['continuous']:
		print 'Deleting the ddocs used to filter the replications.\n'
		db_index = 0
		while db_index < len(dbs):
			source_db = dbs[db_index]
			try:
				fm.remove_filter_func(config['source_url'] + source_db, config['source_auth'])
				db_index += 1
			except FilterError as fe:
				logging.log(fe.level, '{0}\n{1}'.format(fe.msg, json.dumps(fe.r, indent=4)))
				num_failed_ddocs += 1
				db_index += 1
			except:
				print 'Unexpected Error!  View the log for details.'
				logging.error('Unexpected error occured! Error: {0}'.format(sys.exc_info()))
				sys.exit()

	# place the number of failures in to the output queue
	return [num_failed_repl, num_failed_ddocs]



# Print the welcome text and verify current configuration settings
def printWelcome():
	print 'Cloudant Bulk Replication and Resharding Tool'
	print '============================================='
	print 'Start Time: {0}'.format(start_time)
	print 'Source: "{0}"'.format(config['source'])
	print 'Target: "{0}"'.format(config['target'])
	print 'Mediator: "{0}"'.format(config['mediator'])

	if config['use_default_q']:
		print 'Perform Resharding: False'
	else:
		print 'Perform Resharding: True'
		print 'New Q Value: {0}'.format(config['new_q'])

	print 'Skip Design Docs: {0}'.format(config['skip_ddocs'])
	print 'Rename Databases on Target: {0}'.format(config['rename_dbs'])

	if config['incremental_id'] is not None:
		print 'Incremental Replications: True'
		print 'Previous Batch ID: {0}'.format(config['incremental_id'])

	print 'Number of Concurrent Replications: {0}'.format(config['concurrency_limit'])
	print '=============================================\n'

	selection = raw_input('Is this correct? (y/N):')
	if selection != 'y' and selection != 'Y':
		print 'Bye!'
		sys.exit()



# Accepts:	1) An array of command line arguments
# Returns:	Void
def parse_ops(argv):
	try:
		opts, args = getopt.getopt(argv, 's:t:m:dl:fi:zc:p:q:ow:b:C:T:h')
	except getopt.GetoptError:
		print usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-s':
			config['source'] = arg
		elif opt == '-t':
			config['target'] = arg
		elif opt == '-m':
			config['mediator'] = arg
		elif opt == '-d':
			config['rename_dbs'] = True
		elif opt == '-l':
			config['db_list'] = read_db_list(arg)
		elif opt == '-f':
			config['skip_ddocs'] = True
		elif opt == '-i':
			config['incremental_id'] = arg
		elif opt == '-z':
			config['force_concurrency_limit'] = False
		elif opt == '-c':
			config['concurrency_limit'] = int(arg)
		elif opt == '-p':
			config['polling_delay'] = int(arg)
		elif opt == '-q':
			config['new_q'] = arg
			config['use_default_q'] = False
		elif opt == '-o':
			config['continuous'] = True
		elif opt == '-w':
			config['worker_processes'] = int(arg)
		elif opt == '-b':
			config['worker_batch_size'] = int(arg)
		elif opt == '-C':
			config['http_connections'] = int(arg)
		elif opt == '-T':
			config['connection_timeout'] = int(arg)
		elif opt == '-h':
			print usage
			sys.exit()



# Pipe the contents of the filename in to a list
def read_db_list(filename):
	with open(filename) as f:
		content = [x.strip('\n') for x in f.readlines()]
		return content



# Verify source and target are populated.  Check for mediator.  Build URLs.
def init_config():
	if config['source'] == '':
		print usage
		sys.exit()
	if config['target'] == '':
		print usage
		sys.exit()

	if config['mediator'] == '':
		config['mediator'] = config['source']

	config['source_url'] = 'https://{0}.cloudant.com/'.format(config['source'])
	config['target_url'] = 'https://{0}.cloudant.com/'.format(config['target'])
	config['mediator_url'] = 'https://{0}.cloudant.com/'.format(config['mediator'])



# Prepare the auth configurations for source, target, and mediator
def init_auth():
    source_pwd = getpass.getpass('Password for {0}:'.format(config['source']))
    config['source_auth'] = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(config['source'], source_pwd)))

    target_pwd = source_pwd
    if config['target'] != config['source']:
    	target_pwd = getpass.getpass('Password for {0}:'.format(config['target']))
    config['target_auth'] = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(config['target'], target_pwd)))

    mediator_pwd = source_pwd
    if config['mediator'] != config['source']:
    	mediator_pwd = getpass.getpass('Password for {0}:'.format(config['mediator']))
    config['mediator_auth'] = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(config['mediator'], mediator_pwd)))



# Program logic starts here
def main(argv):
	parse_ops(argv)
	init_config()

	logging.info('\n\n=============================================')
	logging.info('Start Time: {0}'.format(start_time))
	printWelcome()
	init_auth()
	logging.info('Configuration: \n{0}'.format(json.dumps(config, indent=4)))

	# locate all dbs for source
	print '\nReading dbs requested for replication...'
	dbs = config['db_list']
	if len(dbs) == 0:
		dbs = rm.get_dbs(config['source_url'], config['source_auth'])
	print 'Retrieved {0} dbs.  Beginning the replication process...'.format(len(dbs))

	# create the _replicator db on the source if it doesn't already exist
	rm.create_replicator(config['mediator_url'], config['mediator_auth'])

	# deploy the ddocs on the _replicator db
	running_repl_url = mm.create_repl_index(config['mediator_url'], config['mediator_auth'])

	# time to start posting replications
	batch_id = int(time.time())
	results = repl_dispatcher(dbs, running_repl_url, batch_id)
	num_failed_repl = results[0]
	num_failed_ddocs = results[1]

	# we're done replicating the list of databases
	print '\n============================================='
	print 'Processing complete.'
	print 'Start Time: {0}'.format(start_time)
	print 'End Time: {0}'.format(time.strftime('%c'))
	print 'Failed replications:  {0}'.format(num_failed_repl)
	print 'Failed filter ddocs:  {0}'.format(num_failed_ddocs)
	print '\nUse *{0}* for incremental replications built off this batch.\n'.format(batch_id)
	logging.info('\n=============================================')
	logging.info('Start Time: {0}'.format(start_time))
	logging.info('End Time: {0}'.format(time.strftime('%c')))
	logging.info('Failed replications:  {0}'.format(num_failed_repl))
	logging.info('Failed filter ddocs:  {0}'.format(num_failed_ddocs))
	logging.info('Use {0} for incremental replications built off this batch.'.format(batch_id))


if __name__ == "__main__":
	main(sys.argv[1:])