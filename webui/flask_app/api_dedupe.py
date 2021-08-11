"""
Simple Flask App

"""

from flask import request, g, jsonify, abort, render_template, redirect
from flask_admin.contrib.mongoengine import ModelView
from datetime import date, datetime
from flask_admin.model import typefmt
from flask_admin.model.template import EndpointLinkRowAction, LinkRowAction

from flask_app import app, models, webui, auth_required, AUTHLVL, subs

import os
import string

# # # # # # # # # # # # # # # # # # # #

def next_dupes():
	rtn = []

	last_fobj = 0
	last_val = 0
	for fobj in models.FileObj.objects.order_by('checksum.sha1'):
		cur_val = fobj.checksum['sha1']
		if( last_val == cur_val ):
			if( cur_val != '__PENDING__' ):
				rtn.append( {
					'obj1': last_fobj,
					'obj2': fobj
				} )
		last_fobj = fobj
		last_val = cur_val

		if( len(rtn) > 9 ):
			break

	return rtn

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@app.route( app.config['BASEPATH']+'dedupe/', methods=['GET'] )
@auth_required( AUTHLVL.ADMIN )
def dedupe1():
	rtn = { 'info':'unknown error', 'status':'error' }

	dupes = next_dupes()
	if( len(dupes) > 0 ):
		#app.logger.error( 'rtn is '+str(dupe) )
		return render_template( 'dedupe1.html', objlist=dupes )

	return 'None'

@app.route( app.config['BASEPATH']+'dedupe/del', methods=['POST'] )
@auth_required( AUTHLVL.ADMIN )
def dedupe_del():
	op = request.form.get('op')
	app.logger.error('dedupe/del op='+op )
	if( op == 'Delete Obj1' ):
		logical_path = request.form.get('obj1')
	elif( op == 'Delete Obj2' ):
		logical_path = request.form.get('obj2')
	app.logger.error( 'dedupe/del called on '+op+'::'+logical_path )
	return render_template( 'dedupe2.html', logical_path=logical_path )

@app.route( app.config['BASEPATH']+'dedupe/delyes', methods=['POST'] )
@auth_required( AUTHLVL.ADMIN )
def dedupe_delyes():
	logical_path = request.form.get('logical_path')
	op = request.form.get('op')
	if( op == 'No' ):
		#app.logger.error( 'dedupe/delyes will NOT delete anything' )
		pass
	elif( op == 'YES Delete!' ):
		#app.logger.error( 'dedupe/delyes called on '+logical_path )
		try:
			os.remove( 'Z:\\'+logical_path )
			fobj = models.FileObj.objects.get( logical_path=logical_path )
			fobj.delete()
		except Exception as e:
			app.logger.error( 'ERROR attempting to remove file: '+str(e) )

	return redirect( '.', code=302 )

