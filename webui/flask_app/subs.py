
"""
Simple Flask App

"""

from flask import request, g, jsonify, abort
from flask_admin.contrib.mongoengine import ModelView

from flask_app import app

import os
import string

# # # # # # # # # # # # # # # # # # # #

# TODO: no real error handling!
def parse_to_dict( inobj ):
	rtn = {}
	if( type(inobj) is dict ):
		rtn = inobj
	if( type(inobj) is list ):
		for k in inobj:
			rtn[k] = True
	elif( type(inobj) is str ):
		# assume this is meant to be foo(=True)
		rtn[inobj] = True
	# else silently fail to let the rest of the system keep going
	return rtn
