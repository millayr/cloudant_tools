# Author:  Ryan Millay, SE - Cloudant
# Initiate a bulk list of replications between account(s).  Allows for migrating a set of dbs to a new q.

import json
import base64
import logging
import sys
import time
import multiprocessing.dummy as multiprocessing
from ExceptionsModule import ReplError
from ExceptionsModule import FatalError
from ExceptionsModule import FilterError
import ReplicationModule as rm
import FilteringModule as fm

##### SCRIPT INSTRUCTIONS #####
# This script will replicate a set of databases owned by source to a new set of databases owned
# by target.  The script allows for changes to Q values if.
#
# 1) Enter in the credentials for source/source_pwd and target/target_pwd.  They can be the same.
#		Be sure to set the mediator of the replication as well.  This allows you to use a third account
#		(third party) to manage the replications between the source and target.  By default, the
#		mediator is configured to be the source account.
#
# 2) 'rename_dbs' --> Determine if you want to append a date to the new database names.  This option 
#		will likely be utilized when the accounts are the same and you need unique database names.
#
# 3) 'repl_all' --> Determine whether you want to migrate ALL databases owned by source to a
#		new Q value.  When set to True, the script will get _all_dbs from source and mark them all
#		as slated for replication.  Also, when set to True, the 'db_list' variable is not used.
#
# 4) 'db_list' --> An array of db names owned by source that you would like replicated over to
#		target.  Note that when 'repl_all' is set to True, this field is ignored.
#
# 5) 'skip_ddocs' --> Allows you to filter out design docs during the replication to prevent
#		index builds on the target database.  Set to True to enable.
#
# 6) 'concurrency_limit' --> The script will ensure that it does not overload the cluster.  This
#		variable represents the upper limit for concurrent replications.  Use care when modifying.
#
# 7) 'use_default_q' --> When set to True, databases on target will be created using the cluster
#		default shard configuration.  When set to False, databases on target will be created
#		with a new q value determined by 'new_q'.
#
# 8) 'new_q' --> The new shard configuration for the databases owned by target.  This field is
#		ignored when 'use_default_q' is set to True.
#
# 9) 'repl_options' --> Use this variable for any extra replication settings you would like.  By
#		default, the replications are not continuous and we are not creating the database on the
#		target account.  If you decide to make the replications continuous, be sure to modify the
#		concurrency_limit to allow for more active replications.
#
# 10) 'force_triggered' --> When set to True, the script will attempt to verify all posted
#		replications have either triggered or completed.
#
# 11) 'retry_delay' --> Number of seconds to sleep before rechecking a replication that has not
#		triggered or completed yet.
#
# 12) 'num_threads' --> Number of threads used to process all of the databases slated for replication.
#		This value must be greater than or equal to 1.  Beware of the law of diminishing returns.


##### EDITABLE VARIABLES #####
source = 'millayr-bulk'
source_pwd = 'SJ_class07'
target = 'millayr-test'
target_pwd = 'SJ_class07'
mediator = source
mediator_pwd = source_pwd
rename_dbs = False
repl_all = True
db_list = ['add', 'specific', 'databases', 'here']
skip_ddocs = False
force_concurrency_limit = True
concurrency_limit = 10
use_default_q = True
new_q = "8"
repl_options = {'continuous': False, 'create_target': True}
force_triggered = True
retry_delay = 5
num_threads = 3

##### DO NOT EDIT #####
source_auth = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(source, source_pwd)))
target_auth = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(target, target_pwd)))
mediator_auth = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(mediator, mediator_pwd)))
source_url = 'https://{0}.cloudant.com/'.format(source)
target_url = 'https://{0}.cloudant.com/'.format(target)
mediator_url = 'https://{0}.cloudant.com/'.format(mediator)
logging.basicConfig(filename='replication.log',level=logging.DEBUG)



# Accepts:	1) An array of dbs to process,
#			2) An output queue for message passing,
#			3) A failure queue for message passing
# Returns:	Void
def repl_dispatcher(dbs, output, failures):
	db_index = 0
	num_failed_repl = num_failed_ddocs = 0
	db_date = int(time.time())
	replications = []

	while db_index < len(dbs):
		try:
			# we only spawn new replications if it's below the limit.  Don't want
			# to overload the cluster. 
			if not force_concurrency_limit or rm.poll_active_repl(mediator_url, mediator_auth, concurrency_limit):

				source_db = target_db = dbs[db_index]

				# make the target db name unique if required
				if rename_dbs:
					target_db += '-{0}'.format(db_date)

				# attempt to create the target db with the new q value if desired
				if not use_default_q:
					rm.create_new_db(target_url, target_db, target_auth, new_q)

				# build a replication doc
				repl_source = source_url + source_db
				repl_target = target_url + target_db
				doc = rm.create_repl_doc(repl_source, source_auth, repl_target, target_auth, mediator, repl_options)

				# create a design document for filtering ddocs if desired
				if skip_ddocs:
					ddoc = fm.create_filter_func(source_url + source_db, source_auth)
					doc.update({'filter': '{0}/{1}'.format(ddoc['name'], ddoc['func'])})

				# post the doc to the source db (i.e. the mediator)
				rm.post_repl_doc(mediator_url, doc, mediator_auth)
				replications.append(doc['_id'])

				# increment index in to array of dbs
				db_index += 1

			else:
				# sleep for an arbitrary amount of time before polling again
				print 'Concurrent replication limit reached...waiting...'
				time.sleep(10)

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
			print 'THREAD HAS DIED!  View the log for details.'
			logging.error('Unexpected error occured! Error: {0}'.format(sys.exc_info()))
			sys.exit()

	# if we're forcing a triggered state, we verify each
	if force_triggered:
		repl_index = 0
		while repl_index < len(replications):
			try:
				rm.confirm_triggered(mediator_url, mediator_auth, replications[repl_index], retry_delay)
			except ReplError as re:
				logging.log(re.level, '{0}\n{1}'.format(re.msg, json.dumps(re.r, indent=4)))
				num_failed_repl += 1
			except:
				print 'THREAD HAS DIED!  View the log for details.'
				logging.error('Unexpected error occured! Error: {0}'.format(sys.exc_info()))
				sys.exit()
			finally:
				repl_index += 1

	# place the number of failures in to the output queue
	output.put([num_failed_repl, num_failed_ddocs])



# Accepts:	1) An array,
#			2) A chunk size to split the array by
# Returns: 	An array of arrays that are of length 'size'
def slice_array(array, size):
	if size < 1:
		size = 1
	return [array[i:i + size] for i in range(0, len(array), size)]



# Print the welcome text and verify current configuration settings
def printWelcome():
	print 'Cloudant Bulk Replication and Resharding Tool'
	print '============================================='
	print 'Source: "{0}"'.format(source)
	print 'Target: "{0}"'.format(target)
	print 'Mediator: "{0}"'.format(mediator)

	if use_default_q:
		print 'Perform Resharding: False'
	else:
		print 'Perform Resharding: True'
		print 'New Q Value: {0}'.format(new_q)

	print 'Skip Design Docs: {0}'.format(skip_ddocs)
	print 'Rename Databases on Target: {0}'.format(rename_dbs)
	print 'Verify Replications Have Triggered: {0}'.format(force_triggered)
	print 'Number of Threads: {0}'.format(num_threads)
	print '=============================================\n'

	selection = raw_input('Is this correct? (y/N):')
	if selection != 'y' and selection != 'Y':
		print 'Bye!'
		sys.exit()



# Program logic starts here
def main():
	logging.info('\n\n=============================================')
	printWelcome()

	# locate all dbs for source
	print '\nReading dbs requested for replication...'
	dbs = db_list
	if repl_all:
		dbs = rm.get_dbs(source_url, source_auth)
	print 'Retrieved {0} dbs.  Beginning the replication process...'.format(len(dbs))

	# create the _replicator db on the source if it doesn't already exist
	rm.create_replicator(mediator_url, mediator_auth)

	# time to kick off some threads to speed up the work
	threads = []
	range_size = len(dbs) // num_threads
	db_chunks = slice_array(dbs, range_size)
	output = multiprocessing.Queue()
	failures = multiprocessing.Queue()
	print 'Configured for {0} threads, each responsible for {1} replications...'.format(num_threads, range_size)
	logging.info('Configured for {0} threads, each responsible for {1} replications...'.format(num_threads, range_size))

	# iterate over each chunk of dbs and spawn a new thread
	for chunk in db_chunks:
		t = multiprocessing.Process(target=repl_dispatcher, args=(chunk, output, failures))
		threads.append(t)
		t.start()

	# join all threads
	for t in threads:
		t.join()

	# notify of any fatal errors that happened
	while not failures.empty():
		failure = failures.get()
		print 'Thread aborted while processing {0}.  View the logs for more info.'.format(failure[1])
		logging.error('Thread responsible for the following DBs aborted while processing {0}:\n{1}'.format(failure[1], failure[0]))

	num_failed_repl = num_failed_ddocs = 0
	if not output.empty():
		for t in threads:
			result = output.get()
			num_failed_repl += result[0]
			num_failed_ddocs += result[1]
	else:
		print 'Unexpected errors occured.  Please check the log for details.'
		sys.exit()


	# we're done replicating the list of databases
	print '\n============================================='
	print 'Processing complete.'
	print 'Failed replications:  {0}'.format(num_failed_repl)
	print 'Failed filter ddocs:  {0}'.format(num_failed_ddocs)
	logging.info('\n=============================================')
	logging.info('Failed replications:  {0}'.format(num_failed_repl))
	logging.info('Failed filter ddocs:  {0}'.format(num_failed_ddocs))


if __name__ == "__main__":
	main()