

import time
import logging

from celery import Celery
import mongoengine as me
from repos_common import init_logs, config_defaults

# TODO: read config from a file
config = config_defaults

# creates/configures mcc_main and mcc_celery log systems
init_logs( config )

# TODO: check for errors
celapp = Celery( 'worker_subs', broker=config['celery_url'] )
print( dir(celapp) )

#print( celapp.tasks )

# to set which queues the worker takes tasks from:
#   celery -A repos_worker worker --pool=solo -l info -Q list,of,queues
# or:  celapp.select_queues( queues=['blue','red','celery'] )

# Inspect all nodes.
insp = celapp.control.inspect()
# print( dir(insp) )

try:
	# Show the items that are in the queue
	pass
	# print( 'registered:' )
	# lst = insp.registered()
	# for key,val in lst.items():
	# 	print(key,val)

	# Show the items that have worker actively running them
	# print( 'active_queues:' )
	# lst = insp.active_queues()
	# for key,val in lst.items():
	# 	print(key,val)

	# Show the items that have an ETA or are scheduled for later processing
	# print( 'registered_tasks:' )
	# lst = insp.registered_tasks()
	# for key,val in lst.items():
	# 	print(key,val)

	# Show the items that have an ETA or are scheduled for later processing
	# print( 'scheduled:' )
	# lst = insp.scheduled()
	# for key,val in lst.items():
	# 	print(key,val)

	# Show tasks that are currently active
	# print( 'active:' )
	# lst = insp.active()
	# for key,val in lst.items():
	# 	print(key,val)

	# Show tasks that have been claimed by workers
	# print( 'reserved' )
	# lst = insp.reserved()
	# for key,val in lst.items():
	# 	print(key,val)

except Exception as e:
	print( str(e) )


# TODO: check for errors
# TODO: this is a hack - we know that the db-name is 'celery' just 'cuz
me.connect( 'celery', host=config['database_host'] )

class CeleryTaskObj(me.Document):
	payload  = me.StringField()
	queue    = me.StringField()
	priority = me.IntField()
	meta     = { 'collection':'messages' }

	def __str__(self):
		return 'CeleryTaskObj( queue="%s", priority=%d, payload="%s" )' % (self.queue,self.priority,self.payload)


print( 'raw celery tasks:' )
for cobj in CeleryTaskObj.objects:
	print( str(cobj) )
