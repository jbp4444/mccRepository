"""
Simple Flask App

"""

from flask import request, g, jsonify, abort, render_template
from flask_admin.contrib.mongoengine import ModelView
from datetime import date, datetime
from flask_admin.model import typefmt
from flask_admin.model.template import EndpointLinkRowAction, LinkRowAction

from flask_app import app, models, webui, auth_required, AUTHLVL, subs

import os
import string

# the app instance is created in __init__

# # # # # # # # # # # # # # # # # # # #

def file_query( authuser, query=None, limit=None, offset=None, verbose=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	if( query is None ):
		the_iterator = models.FileObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = models.FileObj.objects( logical_path__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = models.FileObj.objects( logical_path__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = models.FileObj.objects( logical_path__startswith = qqq )
	else:
		the_iterator = models.FileObj.objects( logical_path = query )
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )
	flist = []
	for f in the_iterator:
		if( verbose is None ):
			flist.append( f.logical_path )
		else:
			flist.append( f )
			#flist.append( { 'logical_path':f.logical_path, 'file_size':f.file_size, 'checksum':f.checksum } )
	if( query is None ):
		rtn = { 'info':'list files', 'status':'ok', 'list':flist }
	else:
		rtn = { 'info':'query files', 'status':'ok', 'query':query, 'list':flist }
	return rtn

def file_ckquery( authuser, query=None, verbose=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	if( query is None ):
		rtn = { 'info':'no query given', 'status':'ok' }
	else:
		if( (query[0]=='%') and (query[-1]=='%') ):
			qqq = query[1:-1]
			the_iterator = models.FileObj.objects( checksum__contains = qqq )
		elif( query[0] == '%' ):
			qqq = query[1:]
			the_iterator = models.FileObj.objects( checksum__endswith = qqq )
		elif( query[-1] == '%' ):
			qqq = query[:-1]
			the_iterator = models.FileObj.objects( checksum__startswith = qqq )
		else:
			the_iterator = models.FileObj.objects( checksum = query )
		flist = []
		for f in the_iterator:
			if( verbose is None ):
				flist.append( f.logical_path )
			else:
				flist.append( { 'logical_path':f.logical_path, 'file_size':f.file_size, 'checksum':f.checksum } )
		rtn = { 'info':'query checksum', 'status':'ok', 'ckquery':query, 'list':flist }
	return rtn

def file_read( authuser, logical_path ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FileObj.objects.get( logical_path=logical_path )
		rtn = { 'logical_path':fobj.logical_path, 'file_size':fobj.file_size, 'checksum':fobj.checksum,
				#'copies':fobj.copies,
				#'tags':fobj.tags,
				'info':'file-obj found', 'status':'ok' }
	except Exception as e:
		# TODO: make this a 401 error?
		rtn = { 'info':'object not found', 'status':'error', 'req_logical_path':logical_path }
		app.logger.warning( 'file_read '+str(e) )
	return rtn

def file_create( authuser, logical_path, file_size, checksum, copies=None, tags=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	# does this already exist?
	try:
		fobj = models.FileObj.objects.get( logical_path = logical_path )
		rtn = { 'info':'file already exists', 'status':'error' }
	except:
		# TODO: check that required params exist/make sense
		# TODO: check that copies/tags are dicts
		if( copies is None ):
			copies = {}
		else:
			copies = subs.parse_to_dict( copies )
		if( tags is None ):
			tags = {}
		else:
			tags = subs.parse_to_dict( tags )
		try:
			fobj = models.FileObj( logical_path=logical_path, file_size=file_size, checksum=checksum,
				copies=copies, tags=tags )
			fobj.save()
			rtn = { 'logical_path':fobj.logical_path, 'file_size':fobj.file_size, 
					'checksum':fobj.checksum, 'copies':fobj.copies, 'tags':fobj.tags,
					'info':'file-obj created', 'status':'ok' }
		except Exception as e:
			app.logger.warning( 'file_create '+str(e) )
			rtn = { 'info':'file-obj could not be created', 'status':'error' }
	return rtn

def file_update( authuser, logical_path, file_size=None, checksum=None, copies=None, tags=None ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FileObj.objects.get( logical_path = logical_path )
	except Exception as e:
		# TODO: should we automatically make this obj-create?
		fobj = models.FileObj( logical_path=logical_path )

	if( file_size != None ):
		fobj.file_size = file_size
	if( checksum != None ):
		fobj.checksum = checksum
	# TODO: check that copies/tags are dicts
	if( copies is not None ):
		copies = subs.parse_to_dict( copies )
		for k,v in copies.items():
			if( v is None ):
				fobj.copies.pop( k, None )   # silently ignore KeyError/missing-keys
			else:
				fobj.copies[k] = v     # NOTE: by default this overwrites old data with same key
	if( tags is not None ):
		tags = subs.parse_to_dict( tags )
		for k,v in tags.items():
			if( v is None ):
				fobj.tags.pop( k, None )   # silently ignore KeyError/missing-keys
			else:
				fobj.tags[k] = v     # NOTE: by default this overwrites old data with same key

	try:
		fobj.save()
		rtn = { 'logical_path':fobj.logical_path, 'file_size':fobj.file_size, 'checksum':fobj.checksum,
			'copies':fobj.copies, 'tags':fobj.tags,
			'info':'file-obj updated', 'status':'ok' }
	except Exception as e:
		rtn = { 'info':'could not store object', 'status':'error' }
	return rtn

def file_delete( authuser, logical_path ):
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FileObj.objects.get( logical_path = logical_path )
		fobj.delete()
		rtn = { 'logical_path':logical_path,
				'info':'file-obj deleted', 'status':'ok' }
	except:
		rtn = { 'info':'object does not exist', 'status':'error' }
	return rtn

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@app.route( app.config['APIBASEPATH']+'file/', defaults={'logical_path':None}, methods=['GET'] )
@app.route( app.config['APIBASEPATH']+'file/<slash:logical_path>', methods=['GET'] )
@auth_required( AUTHLVL.USER )
def file_api_get( logical_path ):
	# with the 'slash' encoding, we get empty logical-paths
	if( (logical_path is None) or (not logical_path) ):
		# if no specific logical-path is given, then this is a query (or list) op
		verbose = request.args.get('verbose')    # return all file info at once (not just a list of server_names)
		query = request.args.get('query')
		ckquery = request.args.get('ckquery')
		if( ckquery is not None ):
			return file_ckquery(g.auth_user,ckquery,verbose)
		else:
			limit = request.args.get('limit')
			offset = request.args.get('offset')
			return file_query(g.auth_user,query,limit,offset,verbose)
	if( logical_path[0] is not '/' ):
		logical_path = '/' + logical_path
	return file_read(g.auth_user,logical_path)

@app.route( app.config['APIBASEPATH']+'file/', methods=['POST'] )
@auth_required( AUTHLVL.ADMIN )
def file_api_post():
	jdata = request.get_json()
	logical_path = jdata.get('logical_path')
	file_size    = jdata.get('file_size')
	checksum     = jdata.get('checksum')
	copies       = jdata.get('copies')
	tags         = jdata.get('tags')
	if( logical_path[0] is not '/' ):
		logical_path = '/' + logical_path
	return file_create(g.auth_user,logical_path,file_size,checksum,copies,tags)

# TODO: break out PATCH (update some) and PUT (update all)??
@app.route( app.config['APIBASEPATH']+'file/', defaults={'logical_path':None}, methods=['PUT'] )
@app.route( app.config['APIBASEPATH']+'file/<slash:logical_path>', methods=['PUT'] )
@auth_required( AUTHLVL.ADMIN )
def file_api_put( logical_path=None ):
	jdata = request.get_json()
	# handle bare PUT command (assume path is in json-args)
	# NOTE: with the 'slash' type, we get empty strings (not None)
	if( (logical_path is None) or (logical_path=='') ):
		logical_path = jdata.get('logical_path')
	file_size    = jdata.get('file_size')
	checksum     = jdata.get('checksum')
	copies       = jdata.get('copies')
	tags         = jdata.get('tags')
	if( logical_path[0] is not '/' ):
		logical_path = '/' + logical_path
	return file_update(g.auth_user,logical_path,file_size,checksum,copies,tags)

@app.route( app.config['APIBASEPATH']+'file/<slash:logical_path>', methods=['DELETE'] )
@auth_required( AUTHLVL.ADMIN )
def file_api_delete( logical_path=None ):
	if( logical_path[0] is not '/' ):
		logical_path = '/' + logical_path
	return file_delete(g.auth_user,logical_path)

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@app.route( app.config['BASEPATH']+'file/', methods=['GET'] )
@app.route( app.config['BASEPATH']+'file/<id>', methods=['GET'] )
@auth_required( AUTHLVL.ADMIN )
def file_web_get( id=None ):
	if( id is None ):
		data = file_query(g.auth_user,'%',10,0,True)
		app.logger.error( 'filelist is '+str(data) )
		return render_template( 'list_file.html', filelist=data['list'] )

	#else:
	rtn = { 'info':'unknown error', 'status':'error' }
	try:
		fobj = models.FileObj.objects.get( id=id )
		rtn = { 'logical_path':fobj.logical_path, 'file_size':fobj.file_size, 'checksum':fobj.checksum,
				#'copies':fobj.copies,
				#'tags':fobj.tags,
				'info':'file-obj found', 'status':'ok' }
	except Exception as e:
		# TODO: make this a 401 error?
		rtn = { 'info':'object not found', 'status':'error', 'req_id':id }
		app.logger.warning( 'file_read '+str(e) )
	return render_template( 'show_image.html', item=rtn )


def datetime_format(view, value):
	return value.isoformat()
def date_format(view, value):
	return value.isoformat()
def dict_format(view, value):
	return str(value)

MY_DEFAULT_FORMATTERS = dict(typefmt.BASE_FORMATTERS)
MY_DEFAULT_FORMATTERS.update({
		#date: date_format,
		#datetime: datetime_format,
		dict: dict_format
	})

class FileObjAdmin(ModelView):
	# can_delete = False  # disable model deletion
	# page_size = 50  # the number of entries to display on the list view
	#list_template = 'file_override.html'
	#column_list = [ 'logical_path', 'file_size', 'checksum', 'copies', 'tags', 'https://{{logical_path}}' ]
	column_type_formatters = MY_DEFAULT_FORMATTERS
	column_extra_row_actions = [
		LinkRowAction('glyphicon glyphicon-picture', 'http://localhost:5000/file/{row_id}'),
		#EndpointLinkRowAction('glyphicon glyphicon-test', 'http://foo.com/id={row_id}}')
	]

webui.add_view( FileObjAdmin(models.FileObj) )
