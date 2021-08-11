"""
Simple Flask App

"""

import os
from enum import Enum
from datetime import datetime

import mongoengine

from flask import Flask
from flask.json import JSONEncoder
from flask_admin import Admin
from flask_admin.contrib.mongoengine import ModelView
from werkzeug.routing import PathConverter

app = Flask(__name__)

# load config settings
app.config.update( dict(
	DEBUG = 1,
	SECRET_KEY = 'foobarbaz',
	API_KEY = 'foobarbaz',
	MAX_CONTENT_LENGTH = 1*1024*1024*1024,
	DATABASE = 'maindb',
	DATABASEHOST = 'mongodb://localhost/',
	# Paths should end with '/' !!
	BASEPATH = '/',
	APIBASEPATH = '/api',
	TOKEN_TIMEOUT = 3600
))

# handle datetime entries as strings
class DateTimeEncoder(JSONEncoder):
	def default(self, o):
		if isinstance(o, datetime):
			return o.isoformat()
		return JSONEncoder.default(self, o)
app.json_encoder = DateTimeEncoder

# simple permissions model for "higher-level" functions
# database holds an integer 'permissions' field, which is bitwise-and of:
class AUTHLVL:
	NONE  = 0
	# "standard" user auth levels
	USER  = 1
	ADMIN = 10

# to allow for double-slash in URL (to pass /foo/bar to routes)
# : https://stackoverflow.com/questions/24000729/flask-route-using-path-with-leading-slash
class AllowSlashConverter(PathConverter):
	regex = '.*?'
app.url_map.converters['slash'] = AllowSlashConverter

# initialize the basic web-UI
webui = Admin( app, name='MongoRepos', template_mode='bootstrap3' )

# TODO: check that db file is valid/exists/etc.
mongoengine.connect( app.config['DATABASE'], host=app.config['DATABASEHOST'] )

from . import subs

# from . import models
# models.init_db()
import repos_model as models

from .userauth import auth_required

from . import api_base
#from . import api_user
from . import api_fsys
from . import api_file
from . import api_dedupe

#from . import charts

# allow for /api/file/+/logical/path/to/file to work more smoothly
# e.g. /api/file//foo/bar becomes /api/file/foo/bar
app.url_map.merge_slashes = True
