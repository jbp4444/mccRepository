"""
Simple Flask App

"""

import time
from functools import wraps
from flask import request, abort, g

from flask_app import app, models

# decorator function to test for permissions
def auth_required( auth_level ):
	def real_auth_decorator(view_function):
		@wraps(view_function)
		def wrapper(*args, **kwargs):
			g.token       = 'admin'
			g.auth_user   = models.UserObj( username='admin', password='password', permissions=100 )
			#g.auth_user.save()
			g.auth_userid = 'admin'
			return view_function(*args,**kwargs)
		return wrapper
	return real_auth_decorator

def real_auth_required( auth_level ):
	def real_auth_decorator(view_function):
		@wraps(view_function)
		def wrapper(*args, **kwargs):
			g.token       = None
			g.auth_user   = None
			g.auth_userid = None

			# app.logger.warning( 'trying to authenticate user' )

			# auth by login-form?
			if( ('username' in request.form) & ('password' in request.form) ):
				username = request.form['username']
				password = request.form['password']
				# app.logger.warning( 'login-auth tokens: '+username+','+password )
				if( (username != None) and (password != None) ):
					try:
						user = models.UserObj.objects.get( username=username, password=password )
						g.auth_user   = user
					except:
						pass
					#app.logger.warning( 'found user info:'+str(user) )

			# auth by device auth (token)?
			# elif( 'X-Device-Token' in request.headers ):
			# 	hdrtoken = request.headers['X-Device-Token']
			# 	# app.logger.warning( 'found dev-auth token:'+hdrtoken )
			# 	time_now = int(time.time())
			# 	try:
			# 		token = models.Token.objects.get( token=hdrtoken )
			# 		# app.logger.warning( 'token tied to userid='+token.user.username )
			# 		time_created = token.time_create
			# 		#if( (time_now-time_created) > app.config['TOKEN_TIMEOUT'] ):
			# 		#	# device-token is too old
			# 		#	abort(401)
			# 		g.token       = token
			# 		g.auth_user   = token.user
			# 	except:
			# 		pass

			# auth by http-basic-auth (username,password)?
			elif( request.authorization != None ):
				auth = request.authorization
				# app.logger.warning( 'basic-auth tokens: '+str(auth) )
				if( auth != None ):
					try:
						user = models.UserObj.objects.get( username=auth['username'], password=auth['password'] )
						g.auth_user   = user
					except:
						pass

			#app.logger.warning( 'found user '+str(g.auth_user) )

			if( g.auth_user is not None ):
				# app.logger.warning( 'found user '+str(g.auth_user) )
				# app.logger.warning( 'user data '+str(g.auth_user.permissions)+';'+str(auth_level) )
				if( g.auth_user.permissions >= auth_level ):
					return view_function(*args,**kwargs)

			# else this was a bad auth attempt
			abort(401)
		return wrapper
	return real_auth_decorator
