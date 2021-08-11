
# in one terminal:
#    cd path_to_our_python_files
#    celery -A repos_celery worker --loglevel=info
# Windows can't run parallel/threaded workers, so use:
#    celery -A repos_celery worker --pool=solo -l info

# in another terminal:
#    python runner.py

import time
import logging

from celery import Celery
import mongoengine
from repos_common import init_logs, config_defaults
from repos_model import FilesysObj

# TODO: read config from a file
config = config_defaults

# creates/configures mcc_main and mcc_celery log systems
init_logs( config )

# TODO: check for errors
celapp = Celery( 'worker_subs', broker=config['celery_url'] )

# to set which queues the worker takes tasks from:
#   celery -A repos_worker worker --pool=solo -l info -Q list,of,queues
# or:  celapp.select_queues( queues=['blue','red','celery'] )

# TODO: check for errors
mongoengine.connect( config['database'], host=config['database_host'] )

# cache a list of known file-systems
fsys_list = []
fsys_primary = None
for fs in FilesysObj.objects:
	fsys_list.append( fs )
	if( fs.is_primary ):
		fsys_primary = fs

from repos_worksubs import *

#from repos_filerepair import *

from repos_imgsubs import *
from repos_musicsubs import *
