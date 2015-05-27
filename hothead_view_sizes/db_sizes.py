import requests
import json
import getpass
from tabulate import tabulate

bigwin_dbs = ['baseball', 'basketball', 'events3', 'football', 'footballprototype', 'hockey', 'nhl', 'soccer80', 'stockcar', 'zabbixtest']
raw_dbs = ['space', 'valor', 'valorcmd']
all_dbs = ['baseball', 'basketball', 'events3', 'football', 'footballprototype', 'hockey', 'nhl', 'soccer80', 'stockcar', 'zabbixtest', 'space', 'valor', 'valorcmd']
dbs = all_dbs
url = 'https://hothead.cloudant.com/'
ddoc_query = '_all_docs?startkey="_design"&endkey="_design0"'
account = 'adm-ryanmillay'
pwd = ''
gb_factor = 1000000000

def get_password():
	global pwd
	pwd = getpass.getpass('Password for {0}:'.format(account))

def main():
	get_password()

	output = []
	total_db_active_size = 0
	total_view_active_size = 0
	total_db_file_size = 0
	total_view_file_size = 0

	for db in dbs:
		ddoc_active_size = 0
		ddoc_file_size = 0

		# retrieve database level sizes
		r = requests.get('{0}{1}'.format(url, db), auth=(account, pwd)).json()
		db_active_size = r['sizes']['active']
		db_file_size = r['sizes']['file']

		# verify active size exists
		if db_active_size is not None:
			total_db_active_size += db_active_size
		else:
			db_active_size = 0
		# verify file size exists
		if db_file_size is not None:
			total_db_file_size += db_file_size
		else:
			db_file_size = 0

		# retrieve design docs for this database
		r = requests.get('{0}{1}/{2}'.format(url, db, ddoc_query), auth=(account, pwd)).json()
		for ddoc in r['rows']:
			# retrieve design doc level sizes
			r = requests.get('{0}{1}/{2}/_info'.format(url, db, ddoc['id']), auth=(account, pwd)).json()
			view_active_size = r['view_index']['sizes']['active']
			view_file_size = r['view_index']['sizes']['file']

			#if db == 'stockcar':
			#	print float(view_file_size)/gb_factor

			# verify active size exists
			if view_active_size is not None:
				ddoc_active_size += view_active_size
			# verify file size exists
			if view_file_size is not None:
				ddoc_file_size += view_file_size

		#if db == 'stockcar':
		#	print float(ddoc_file_size)/gb_factor

		total_view_active_size += ddoc_active_size
		total_view_file_size += ddoc_file_size

		output.append([db, float(db_active_size)/gb_factor, float(db_file_size)/gb_factor, 
			float(ddoc_active_size)/gb_factor, float(ddoc_file_size)/gb_factor])

	output.append(['Totals', float(total_db_active_size)/gb_factor, float(total_db_file_size)/gb_factor,
		float(total_view_active_size)/gb_factor, float(total_view_file_size)/gb_factor])
	print tabulate(output, headers=['Name', 'DB Active Size', 'DB File Size', 'DDoc Active Size', 'DDoc File Size'])



if __name__ == "__main__":
	main()
