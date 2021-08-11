"""
Simple Flask App

"""

from flask import request, g, jsonify, abort, render_template
from flask_admin.contrib.mongoengine import ModelView

from flask_app import app, models, webui, auth_required, AUTHLVL, subs

import os
import string

# # # # # # # # # # # # # # # # # # # #

def fsys_query( authuser, query=None, verbose=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	if( query is None ):
		the_iterator = models.FilesysObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = models.FilesysObj.objects( server_name__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = models.FilesysObj.objects( server_name__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = models.FilesysObj.objects( server_name__startswith = qqq )
	else:
		the_iterator = models.FilesysObj.objects( server_name = query )
	flist = []
	for f in the_iterator:
		if( verbose is None ):
			flist.append( f.server_name )
		else:
			flist.append( f )
			#flist.append( {'server_name':f.server_name,'root_dir':f.root_dir,'is_tape':f.is_tape} )
	if( query is None ):
		rtn = { 'info':'list filesystems', 'status':'ok', 'list':flist }
	else:
		rtn = { 'info':'query filesystems', 'status':'ok', 'list':flist }
	return rtn

def fsys_read( authuser, server_name ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FilesysObj.objects.get( server_name=server_name )
		rtn = { 'server_name':fobj.server_name, 'root_dir':fobj.root_dir, 'is_tape':fobj.is_tape,
				'tags':fobj.tags,
				'info':'filesys obj found', 'status':'ok' }
	except:
		# TODO: make this a 401 error?
		rtn = { 'info':'object not found', 'status':'error', 'req_server_name':server_name }
	return rtn

def fsys_create( authuser, server_name, root_dir, is_tape, tags=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	# does this already exist?
	try:
		fobj = models.FilesysObj.objects.get( server_name=server_name )
		rtn = { 'info':'filesys already exists', 'status':'error' }
	except:
		# no matching object exists, so we can create one
		# : convert is_tape to boolean
		if( type(is_tape) == str ):
			if( (is_tape[0]=='Y') or (is_tape[0]=='y') or (is_tape[0]=='T') or (is_tape[0]=='t')
				or (is_tape[0]=='1') ):
				is_tape = True
			else:
				is_tape = False
		# TODO: check that tags is a dict
		if( tags is None ):
			tags = {}
		try:
			tags = subs.parse_to_dict( tags )
			fobj = models.FilesysObj( server_name=server_name, root_dir=root_dir, is_tape=is_tape,
				tags=tags )
			# TODO: check returned value/make sure the data was actually saved
			fobj.save()
			rtn = { 'server_name':fobj.server_name, 'root_dir':fobj.root_dir, 'is_tape':fobj.is_tape,
					'copies':fobj.copies, 'tags':fobj.tags,
					'info':'filesys obj created', 'status':'ok' }
		except:
			rtn = { 'info':'filesys could not be created', 'status':'error' }
	return rtn

def fsys_update( authuser, server_name, root_dir=None, is_tape=None, tags=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FilesysObj.objects.get( server_name = server_name )
		if( root_dir is not None ):
			fobj.root_dir = root_dir
		if( is_tape is not None ):
			fobj.is_tape = is_tape
		# TODO: check that tags is a dict
		if( tags is not None ):
			tags = subs.parse_to_dict( tags )
			for k,v in tags.items():
				if( v is None ):
					fobj.tags.pop( k, None )   # silently ignore KeyError/missing-keys
				else:
					fobj.tags[k] = v     # NOTE: by default this overwrites old data with same key
		# TODO: check returned value
		fobj.save()
		rtn = { 'server_name':fobj.server_name, 'root_dir':fobj.root_dir, 'is_tape':fobj.is_tape,
				'tags':fobj.tags, 
				'info':'filesys obj updated', 'status':'ok' }
	except Exception as e:
		app.logger.warning( 'fsys_update '+str(e) )
		rtn = { 'info':'object does not exist', 'status':'error', 'req_server_name':server_name }
	return rtn

def fsys_delete( authuser, server_name ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FilesysObj.objects.get( server_name = server_name )
		fobj.delete()
		rtn = { 'server_name':server_name, 
				'info':'filesys obj deleted', 'status':'ok' }
	except:
		rtn = { 'info':'object does not exist', 'status':'error' }
	return rtn

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@app.route( app.config['APIBASEPATH']+'fsys/', defaults={'server_name':None}, methods=['GET'] )
@app.route( app.config['APIBASEPATH']+'fsys/<server_name>', methods=['GET'] )
@auth_required( AUTHLVL.USER )
def fsys_api_get( server_name ):
	if( server_name is None ):
		# if no specific server is given, then this is a query (or list) op
		verbose = request.args.get('verbose')    # return all fsys info at once (not just a list of server_names)
		query = request.args.get('query')
		return fsys_query(g.auth_user,query,verbose)
	return fsys_read(g.auth_user,server_name)

@app.route( app.config['APIBASEPATH']+'fsys/', methods=['POST'] )
@auth_required( AUTHLVL.ADMIN )
def fsys_api_post():
	jdata = request.get_json()
	server_name = jdata.get('server_name')
	root_dir    = jdata.get('root_dir')
	is_tape     = jdata.get('is_tape')
	tags        = jdata.get('tags')
	return fsys_create(g.auth_user,server_name,root_dir,is_tape,tags)

# TODO: break out PATCH (update some) and PUT (update all)??
@app.route( app.config['APIBASEPATH']+'fsys/', defaults={'server_name':None}, methods=['PUT'] )
@app.route( app.config['APIBASEPATH']+'fsys/<server_name>', methods=['PUT'] )
@auth_required( AUTHLVL.ADMIN )
def fsys_api_put( server_name=None ):
	jdata = request.get_json()
	root_dir    = jdata.get('root_dir')
	is_tape     = jdata.get('is_tape')
	tags        = jdata.get('tags')
	return fsys_update(g.auth_user,server_name,root_dir,is_tape,tags)

@app.route( app.config['APIBASEPATH']+'fsys/<server_name>', methods=['DELETE'] )
@auth_required( AUTHLVL.ADMIN )
def fsys_api_delete( server_name=None ):
	return fsys_delete(g.auth_user,server_name)

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@app.route( app.config['BASEPATH']+'fsys/', methods=['GET'] )
@auth_required( AUTHLVL.ADMIN )
def fsys_web_get():
	data = fsys_query(g.auth_user,None,True)
	#app.logger.error( 'fsyslist is '+str(data) )
	return render_template( 'list_fsys.html', fsyslist=data['list'] )


class FilesysAdmin(ModelView):
	# can_delete = False  # disable model deletion
	# page_size = 50  # the number of entries to display on the list view
	pass

webui.add_view( FilesysAdmin(models.FilesysObj) )
