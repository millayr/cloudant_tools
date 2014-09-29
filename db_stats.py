import requests
import json

account = '' # FILL ME IN
auth = ''  # FILL ME IN:  use the form 'Basic LWRxLXJ0YW5tEWxsYXkBVlVfY2xhc3MxMQ=='
url = 'https://{0}.cloudant.com/'.format(account)

def main():
	dbs = requests.get(url + '_all_dbs', headers={'Authorization': auth}).json()
	
	output = []
	for db in dbs:
		print 'Retrieving stats for {0}...'.format(db)
		stats = requests.get(url + db, headers={'Authorization': auth}).json()
		output.append(stats['doc_count'])

	print json.dumps(output, indent=4)
	print json.dumps(sort(output), indent=4)

def sort(array):
    less = []
    equal = []
    greater = []

    if len(array) > 1:
        pivot = array[0]
        for x in array:
            if x < pivot:
                less.append(x)
            if x == pivot:
                equal.append(x)
            if x > pivot:
                greater.append(x)
        # Don't forget to return something!
        return sort(less)+equal+sort(greater)  # Just use the + operator to join lists
    # Note that you want equal ^^^^^ not pivot
    else:  # You need to hande the part at the end of the recursion - when you only have one element in your array, just return the array.
        return array



if __name__ == "__main__":
	main()