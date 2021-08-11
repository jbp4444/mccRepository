
import os
import hashlib
import shutil
import logging
from datetime import datetime

import mongoengine

from repos_worker import celapp, fsys_list, fsys_primary
from repos_model import *
from repos_common import *

# class MyBaseTask( celery.Task ):
# 	def log_error( self, msg ):
# 			print( '** ERROR: '+msg )
# use the base class with: 
#     @celapp.task( base=MyBaseTask, bind=True )
#     def foo( self, arg1, arg2 ): ...

@celapp.task
def dummy_task( message ):
	print( 'dummy_task: '+message )
	return 0


@celapp.task
def copy_file( logicalpath, src_name, dst_name ):
	logger = logging.getLogger( 'mcc-main' )
	logger.info( 'copy-file for '+logicalpath+' from '+src_name+' to '+dst_name )

	# TODO: check that these exist/are correct
	src_server = None
	dst_server = None
	for fs in fsys_list:
		if( fs['server_name'] == src_name ):
			src_server = fs
		if( fs['server_name'] == dst_name ):
			dst_server = fs

	# TODO: check for errors

	# Windows is wonky ... os.path.join( 'C:\foo', 'bar' ) does NOT work correctly!
	# localpath = os.path.join( server['root_dir'], logicalpath )
	src_localpath = os.path.abspath( src_server['root_dir'] + '/' + logicalpath )
	dst_localpath = os.path.abspath( dst_server['root_dir'] + '/' + logicalpath )
	dst_localdir,xxx  = os.path.split( dst_localpath )

	try:
		os.makedirs( dst_localdir, exist_ok=True )
		shutil.copyfile( src_localpath, dst_localpath )
	except Exception as e:
		logger.error( '** ERROR: cannot copy file: '+str(e) )
	else:
		# only executes if the commands run w/o an exception
		fobj = FileObj.objects.get( logical_path=logicalpath )
		fobj.copies[dst_name] = { 'last_cksum': datetime.now().isoformat() }
		fobj.save()

@celapp.task
def ingest_new_file( logicalpath, params ):
	# ingest a new file from the primary filesys
	# : does NOT copy it to remote fsys!
	# : assume this gets run nightly on primary file-sys (only on primary)
	logger = logging.getLogger( 'mcc_main' )
	rtn = 0
	server_name = params['server_name']
	ckalgo = params['cksum_algo']

	localpath = to_local_path( logicalpath, server_name )
	logger.info( 'ingest new file for '+logicalpath+' on server '+server_name+' localpath is '+localpath )

	to_process = False
	try:
		# the file-obj should exist, at least as a sentinel
		fobj = FileObj.objects.get( logical_path=logicalpath )
		if( fobj.file_size < 0 ):
			to_process = True

		if( ckalgo in fobj.checksum ):
			if( fobj.checksum[ckalgo] == '__PENDING__' ):
				to_process = True
		else:
			# we've never seen this cksum-algo .. need to calc it
			to_process = True

		if( server_name in fobj.copies ):
			if( fobj.copies[server_name] == '__PENDING__' ):
				to_process = True
		else:
			# we've never seen this copy of this file before
			to_process = True

	except mongoengine.DoesNotExist as e:
		to_process = True
		fobj = None
	except Exception as e:
		logger.error( '   ERROR cannot read file-obj info '+str(e) )
		rtn = -1

	if( to_process ):
		cksum = calc_cksum( localpath )
		fsize = os.path.getsize( localpath )
		logger.info( '   cksum on '+logicalpath+' is '+cksum )
		if( fobj is None ):
			checksum = { ckalgo: cksum }
			copies = { server_name: params['timestamp'] }
			fobj = FileObj( logical_path=logicalpath, file_size=fsize, checksum=checksum,
				copies=copies )
		else:
			# TODO: verify the cksum and file-size are correct!
			fobj.file_size = fsize
			fobj.checksum[ckalgo] = cksum
			fobj.copies[server_name] = { 'last_cksum': params['timestamp'] }
		fobj.save()
		# TODO: we could issue the copy-commands here too, to push to remote file systems

	return rtn

@celapp.task
def verify_existing_file( logicalpath, params ):
	# verifies that a file-sys has copies of all known files
	# : does NOT initiate copy commands for missing files
	# : assume this runs monthly on all file-sys (primary and non-primary)
	logger = logging.getLogger( 'mcc_main' )
	rtn = 0
	
	# TODO: check that required params exist
	server_name = params['server_name']
	timestamp   = params['timestamp']
	ckalgo      = params['cksum_algo']

	# convert to localpath so we can cksum it
	localpath = to_local_path( logicalpath, server_name )
	logger.info( 'verify existing file for '+logicalpath+' on server '+server_name+'  localpath is '+localpath )

	if( os.path.isfile(localpath) ):
		cksum = calc_cksum( localpath )
		fsize = os.path.getsize( localpath )
	else:
		# file does not exist!
		logger.warning( '* ERROR: file does not exist (%s,%s)'%(logicalpath,server_name) )
		# TODO: could call a copy-cmd to push the file to the current server
		return -99

	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		if( fobj.file_size != fsize ):
			logger.error( '   ERROR - file sizes do not match' )
			rtn = -1
		else:
			logger.info( 'file-size match for '+logicalpath )
		checksum = fobj.checksum
		if( ckalgo in checksum ):
			if( checksum[ckalgo] != cksum ):
				logger.error( '   ERROR - checksums do not match' )
				rtn = -1
			else:
				logger.info( 'cksum match for '+logicalpath )
		else:
			# new cksum-algo for this file ... no data to compare to
			logger.warning( '   Warning - no checksum info for %s .. skipping'%(ckalgo) )
	except Exception as e:
		logger.error( '   ERROR cannot read file-obj info' )
		rtn = -1
	return rtn


@celapp.task
def dedupe_file( copy1_name, copy2_name ):
	logger = logging.getLogger( 'mcc-main' )

	dedupe_rootdir = 'C:\\Users\\jbp1\\data\\dedupe'

	logger.info( 'dedupe-file for '+copy1_name+' and '+copy2_name )

	# TODO: find primary (all?) servers that have a copy of the file(s)
	server = fsys_primary
	root_dir = fsys_primary.root_dir

	# Windows is wonky ... os.path.join( 'C:\foo', 'bar' ) does NOT work correctly!
	# localpath = os.path.join( server['root_dir'], logicalpath )
	copy1_path = os.path.abspath( root_dir + '/' + copy1_name )
	copy2_path = os.path.abspath( root_dir + '/' + copy2_name )
	dedupe_path = os.path.abspath( dedupe_rootdir + '/' + copy2_name )
	dedupe_dir,xxx  = os.path.split( dedupe_path )

	try:
		os.makedirs( dedupe_dir, exist_ok=True )
		shutil.move( copy2_path, dedupe_path )
		logger.info( '  moved '+copy2_path+' to '+dedupe_path )
	except Exception as e:
		logger.error( '** ERROR: cannot move file: '+str(e) )
	else:
		# only executes if the commands run w/o an exception
		# fobj = FileObj.objects.get( logical_path=copy2_name )
		# fobj.copies[dst_name] = { 'last_cksum': datetime.now().isoformat() }
		# fobj.save()
		copy2_obj = FileObj.objects( logical_path=copy2_name )
		copy2_obj.delete()
		logger.info( '  removed copy2_obj '+str(copy2_obj) )
