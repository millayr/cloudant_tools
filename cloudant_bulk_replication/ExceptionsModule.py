# Author:  Ryan Millay, SE - Cloudant
# This file defines the exceptions used in the cloudant_bulk_replication utility

# Exception classes
class ReplError(Exception):
	def __init__(self, msg, level, r=[]):
		self.msg = msg
		self.level = level
		self.r = r

class FatalError(Exception):
	def __init__(self, msg, level):
		self.msg = msg
		self.level = level

class FilterError(Exception):
	def __init__(self, msg, level, r=[]):
		self.msg = msg
		self.level = level
		self.r = r