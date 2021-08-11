"""
Simple Flask App

"""

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# THIS FILE IS NOT CURRENTLY USED !!!!
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from flask import request, g, jsonify, abort

from flask_app import app, db, auth_required, AUTHLVL

import os
import string
import subprocess
import glob

# the app instance is created in __init__

# # # # # # # # # # # # # # # # # # # #

def get_obj_data( authuser, path, metadata ):
	rtn = { 'info':'unknown error', 'status':'error' }
	# This should be an existing object
	# : first, check if the server-name exists (for filesys-obj)
	server_name = None
	if( 'server_name' in metadata ):
		server_name = metadata['server_name']
	#   : and does it exist in filesys table
	fsysobj = db.FilesysObj.get_or_none( db.FilesysObj.server_name == server_name )
	if( fsysobj == None ):
		rtn = { 'info':'server_name is not known', 'status':'error' }
	else:
		fobj = db.FileObj.get_or_none( db.FileObj.pathname == path )
		if( fobj == None ):
			# TODO: make this a 401 error?
			rtn = { 'info':'file-object not found', 'status':'error' }
		else:
			finst = db.FileInst.get_or_none( (db.FileInst.filesys_obj==fsysobj) & (db.FileInst.file_obj==fobj) )
			if( finst == None ):
				rtn = { 'info':'file-instance not found', 'status':'error' }
			else:
				rtn = { 'info':'read file-instance', 'status':'ok', 'pathname':path, 
					'file_size':fobj.file_size, 'cksum':fobj.cksum, 'server_name':fsysobj.server_name,
					'root_dir':fsysobj.root_dir, 'is_tape':fsysobj.is_tape }
	return rtn

def update_obj_data( authuser, path, metadata ):
	rtn = { 'info':'unknown error', 'status':'error' }
	fobj = db.FileObj.get_or_none( db.FileObj.pathname == path )
	if( fobj == None ):
		# This is a new object
		fobj = db.FileObj( pathname=path, file_size=metadata['file_size'], cksum=metadata['cksum'] )
		fobj.save()
		rtn = { 'info':'new object created', 'status':'ok', 'pathname':path, 
			'file_size':fobj.file_size, 'cksum':fobj.cksum  }
	else:
		if( 'file_size' in metadata ):
			# app.logger.info( ' .. updating file_size' )
			fobj.file_size = metadata['file_size']
		if( 'cksum' in metadata ):
			# app.logger.info( ' .. updating cksum' )
			fobj.cksum = metadata['cksum']
		fobj.save()
		rtn = { 'info':'wrote file metadata', 'status':'ok', 'pathname':path, 
			'file_size':fobj.file_size, 'cksum':fobj.cksum }
		# TODO: check FileInst table for related instances of this obj
	return rtn

def put_obj_data( authuser, path, metadata ):
	rtn = { 'info':'unknown error', 'status':'error' }
	# This should be a new object
	# : first, check if the server-name exists (for filesys-obj)
	server_name = None
	if( 'server_name' in metadata ):
		server_name = metadata['server_name']
	#   : and does it exist in filesys table
	fsysobj = db.FilesysObj.get_or_none( db.FilesysObj.server_name == server_name )
	if( fsysobj == None ):
		rtn = { 'info':'server_name is not known', 'status':'error' }
	else:
		fobj = db.FileObj.get_or_none( db.FileObj.pathname == path )
		if( fobj == None ):
			# this is a new file-obj (as well as new file-instance)
			fobj = db.FileObj( pathname=path, file_size=metadata['file_size'], cksum=metadata['cksum'] )
			fobj.save()
		# now we can create the new file-instance
		# : note that it is impossible for a file-inst to exist w/o the corresponding file-obj
		finst = db.FileInst.get_or_none( (db.FileInst.filesys_obj==fsysobj) & (db.FileInst.file_obj==fobj) )
		if( finst != None ):
			# this is trying to overwrite an existing file-inst
			rtn = { 'info':'file already exists on server', 'status':'error' }
		else:
			finst = db.FileInst( filesys_obj=fsysobj, file_obj=fobj )
			finst.save()
			# : return some relevant info
			rtn = { 'info':'new file-instance created', 'status':'ok', 'pathname':path, 
				'file_size':fobj.file_size, 'cksum':fobj.cksum, 'server_name':server_name }
	return rtn

# # #

@app.route( app.config['BASEPATH']+'data/<path:path>', methods=['GET'] )
@auth_required( AUTHLVL.USER )
def api_get_data( path=None ):
	if( path == None ):
		abort( 501 )
	else:
		metadata = {}
		if( request.json != None ):
			metadata = request.json
		return jsonify( get_obj_data(g.auth_user,path,metadata) )

@app.route( app.config['BASEPATH']+'data/<path:path>', methods=['PUT'] )
@auth_required( AUTHLVL.USER )
def api_put_data( path=None ):
	if( path == None ):
		abort( 501 )
	else:
		metadata = {}
		if( request.json != None ):
			metadata = request.json
		return jsonify( put_obj_data(g.auth_user,path,metadata) )

@app.route( app.config['BASEPATH']+'data/<path:path>', methods=['POST'] )
@auth_required( AUTHLVL.USER )
def api_post_data( path=None ):
	if( path == None ):
		abort( 501 )
	else:
		metadata = {}
		if( request.json != None ):
			metadata = request.json
		return jsonify( update_obj_data(g.auth_user,path,metadata) )
