"""
Simple Flask App

"""

from flask import abort, g, request, json, jsonify, Response, render_template, send_file

from flask_app import app

# the app instance is created in __init__


@app.errorhandler( 400 )
def handle_bad_req(e):
	return jsonify( {'status':400, 'info':'bad request'} ), 400
@app.errorhandler( 401 )
def handle_bad_auth(e):
	if( request.path.startswith('/api/') ):
		return jsonify( {'status':401, 'info':'authentication failure'} ), 401
	else:
		return Response( 'You have to login with proper credentials', 401,
			{'WWW-Authenticate': 'Basic realm="Login Required"'} )

@app.route( app.config['BASEPATH']+'help', methods=['GET'] )
def show_endpoints():
	rtn = [ {'endpoint':'/logs', 'verb':'GET', 'info':'show the list of available dates with logs'},
		{'endpoint':'/logs/<YYYYMMDD>', 'verb':'GET', 'info':'show the list of log files for a given date'},
		{'endpoint':'/logs/<YYYYMMDD>/<logfile>', 'verb':'GET', 'info':'get a specfici log file for a given date'},
		{'endpoint':'/search', 'verb':'POST', 'info':'search the logs for a given query'} ]
	return jsonify(rtn)

# quick test
@app.route( app.config['BASEPATH']+'test/' )
@app.route( app.config['BASEPATH']+'test/<foo>' )
def api_test( foo=None ):
	rtn = { 'info':'api test', 'status':'ok', 'foo':str(foo) }
	app.logger.warning( 'api_test:'+str(foo) )
	return jsonify(rtn)

# ignore favicon.ico requests (from flask-admin web-ui)
@app.route( '/favicon.ico' )
def catch_favicon():
	return ''

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
	return render_template( 'hello.html', name=name )

@app.route( app.config['BASEPATH']+'img/<path:logical_path>' )
def get_raw_file( logical_path ):
	fname = 'Z:\\' + logical_path
	#app.logger.error( 'img path '+fname )
	return send_file( fname )


# catch-all (mostly for debugging)
# @app.route( '/<path:path>' )
# def catch_all(path):
# 	app.logger.warning( 'BASEPATH is '+app.config['BASEPATH']+' :: path is '+path )
# 	return 'Bzzzt!  Thank you for playing ... %s' % path
