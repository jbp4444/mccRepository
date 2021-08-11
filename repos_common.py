
import os
import sys
import fnmatch
import hashlib
import logging

import mongoengine

from repos_model import *

# # # # # # # # # # # # # # # # # # # #

# list ALL possible options here
config_defaults = {
	'verbose': 0,
	'database': 'maindb',
	'database_host': 'localhost',
	'celery_url': 'mongodb://localhost:27017/celery',
	'celery_logfile': 'c:\\users\\jbp1\\data\\mcc_celery.log',
	'queue': 'red',
	'main_logfile': 'c:\\users\\jbp1\\data\\mcc_main.log',
	'files_default': 'include',
	'files_incl': ['*'],
	'files_excl': [],
	'dirs_default': 'include',
	'dirs_incl': ['*'],
	'dirs_excl': [],
	'cksum_blocksize': 4096,
	'cksum_algo': 'sha1'
	# 'fsys_list': [] .. set by init_cli
	# 'server_name': 'foo' .. set by init_cli
}

# # # # # # # # # # # # # # # # # # # #

def init_logs( cfg ):
	# main File-Operations log
	main_logger = logging.getLogger( 'mcc_main' )
	main_logger.setLevel( logging.INFO )
	# main_logger.handlers.clear()
	# main_logger.handlers = []
	if( not main_logger.hasHandlers() ):
		main_handler   = logging.FileHandler( cfg['main_logfile'] )
		main_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
		main_handler.setFormatter( main_formatter )
		main_logger.addHandler( main_handler )

	# for all the incidental celery log data
	cel_logger = logging.getLogger( 'mcc_celery' )
	cel_logger.setLevel( logging.INFO )
	if( not cel_logger.hasHandlers() ):
		cel_handler   = logging.FileHandler( cfg['celery_logfile'] )
		cel_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
		cel_handler.setFormatter( cel_formatter )
		cel_logger.addHandler( cel_handler )

def init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name ):
	# start with defaults ...	
	for k,v in config_defaults.items():
		ctx.obj[k] = v

	# click.echo( 'app-path:'+click.get_app_dir('repos.py') )

	# read/override with the config file
	if( cfg_file is not None ):
		# TODO: catch errors
		with open(cfg_file,'r') as fp:
			data = json.load(fp)
		# don't want to pollute things (if errors in file)
		# .. just copy known keys over
		for k,v in config_defaults.items():
			if( k in data ):
				ctx.obj[k] = data[k]

	ctx.obj['output_file'] = output_file

	# override verbose level with cmd-line arg
	ctx.obj['verbose'] = verbose

	# TODO: check for errors
	mongoengine.connect( ctx.obj['database'], host=ctx.obj['database_host'] )

	# read/cache the known filesystems
	fsys_list = []
	fsys_primary = None
	fsys_tape = None
	for fs in FilesysObj.objects:
		fsys_list.append( fs )
		if( fs.is_primary ):
			fsys_primary = fs
		if( fs.is_tape ):
			fsys_tape = fs
	ctx.obj['fsys_list'] = fsys_list
	ctx.obj['fsys_primary'] = fsys_primary
	ctx.obj['fsys_tape'] = fsys_tape

	if( server_name is None ):
		if( len(fsys_list) > 0 ):
			# default is to use item-0 .. unless we can find a primary
			ctx.obj['server_name'] = ctx.obj['fsys_list'][0]['server_name']
			for fs in fsys_list:
				if( fs.is_primary ):
					ctx.obj['server_name'] = fs.server_name
					break
		else:
			ctx.obj['server_name'] = None
	else:
		# TODO: check that server_name is in the fsys_list
		ctx.obj['server_name'] = server_name

	# task-queue to use for celery
	if( queue_name is not None ):
		ctx.obj['queue'] = queue_name

	# TODO: read overall mcc config file for log info
	init_logs( config_defaults )
	ctx.obj['logger'] = logging.getLogger( 'mcc_main' )

	# create a logger for the output information
	out_logger = logging.getLogger( 'mcc_output' )
	out_logger.setLevel( logging.INFO )
	if( ctx.obj['output_file'] is None ):
		# remove any existing handlers
		# for h in logger.handlers:
		# 		if isinstance(h, logging.NullHandler):
		# 			logger.removeHandler(h)
		out_logger.addHandler( logging.NullHandler() )
	else:
		if( not out_logger.hasHandlers() ):
			out_handler   = logging.FileHandler( ctx.obj['output_file'] )
			out_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
			out_handler.setFormatter( out_formatter )
			out_logger.addHandler( out_handler )
	ctx.obj['output'] = out_logger

def calc_cksum( infile, opts=None ):
	# TODO: catch errors/bad hash-algo
	if( opts is not None ):
		blksz = opts.obj['cksum_blocksize']
		ckobj = hashlib.new( opts.obj['cksum_algo'] )
	else:
		blksz = config_defaults['cksum_blocksize']
		ckobj = hashlib.new( config_defaults['cksum_algo'] )
	with open( infile,'rb') as f:
		for chunk in iter(lambda: f.read(blksz), b""):
			ckobj.update(chunk)
	return ckobj.hexdigest()

def parse_kvtags( kvtag_list ):
	kvtags = {}
	if( kvtag_list is not None ):
		for kv in kvtag_list:
			i = kv.find('=')
			if( i < 0 ):
				# no key=val, assume a boolean (=True)
				kvtags[kv] = True
			elif( i > 0 ):
				k = kv[:i]
				v = kv[(i+1):]
				# TODO: use python-ish 'None' or use a more obvious/less likely flag instead?
				#   e.g. NONE, _NONE, NULL, _NULL
				if( (v=='NONE') or (v=='NULL') ):
					v = None
				# TODO: use python-ish 'True' and 'False' or use a more obvious/less likely flag instead?
				elif( v=='TRUE' ):
					v = True
				elif( v=='FALSE' ):
					v = False
				elif( v[0].isdigit() ):
					if( '.' in v ):
						v = float( v )
					else:
						v = int( v )
				kvtags[k] = v
			elif( i == 0 ):
				# TODO: if i==0, then string is '=foo' which is an error
				pass
	return kvtags

# # # # # # # # # # # # # # # # # # # #

def to_logical_path( localpath, config, extra=False ):
	if( localpath[0:2] == '//' ):
		# assume files that start //foo/bar are encoding a logical path
		if( extra ):
			return (localpath[1:],config.obj['fsys_list'][0]['server_name'])
		return localpath[2:]

	# find what server has the same root_dir as the leading part of the localpath
	if( '~' in localpath ):
		localpath = os.path.expanduser( localpath )
	abspath = os.path.abspath( localpath )
	#abspath = abspath.lower()
	logicalpath = abspath
	server = None
	for d in config.obj['fsys_list']:
		i = len(d['root_dir'])
		if( abspath[:i] == d['root_dir'] ):
			logicalpath = abspath[i:]
			server = d['server_name']
			break
	logicalpath = logicalpath.replace('\\','/')

	if( extra ):
		return (logicalpath,server)
	return logicalpath

def to_local_path( logicalpath, server_name, config=None ):
	server = None
	if( config is not None ):
		for d in config.obj['fsys_list']:
			if( server_name == d['server_name'] ):
				server = d
				break
	else:
		try:
			fobj = FilesysObj.objects.get( server_name=server_name )
			server = fobj
		except Exception as e:
			click.echo( str(e) )
	# TODO: catch errors (though NoneType should error on [] ref below)

	# Windows is wonky ... os.path.join( 'C:\foo', 'bar' ) does NOT work correctly!
	# localpath = os.path.join( server['root_dir'], logicalpath )
	localpath = os.path.abspath( server['root_dir'] + '/' + logicalpath )
	return localpath

# # # # # # # # # # # # # # # # # # # #

def is_filename_match( filename, config ):
	if( config.obj['files_default'] == 'include' ):
		should_process = True
		for pattern in config.obj['files_excl']:
			if( fnmatch.fnmatch(filename,pattern) ):
				should_process = False
				break
	else:
		should_process = False
		for pattern in config.obj['files_incl']:
			if( fnmatch.fnmatch(filename,pattern) ):
				should_process = True
				break
	return should_process

def is_directory_match( dirname, config ):
	if( config.obj['dirs_default'] == 'include' ):
		should_process = True
		for pattern in config.obj['dirs_excl']:
			if( fnmatch.fnmatch(dirname,pattern) ):
				should_process = False
				break
	else:
		should_process = False
		for pattern in config.obj['dirs_incl']:
			if( fnmatch.fnmatch(dirname,pattern) ):
				should_process = True
				break
	return should_process

# basic func to walk the file-tree and process files
# : calls file_func on each found file (return None if don't need to process)
# : batches the needed-calls together and gets/puts the data to server
# : calls verify_func on each returned value (list) from server
def walk_directory_tree( inpath, file_func, func_data, opts ):
	verbose = opts.obj['verbose']
	progress_bar = opts.obj.get( 'progress_bar' )
	err_count = 0

	for curdir, subdirs, files in os.walk(inpath):
		if( is_directory_match(curdir,opts) ):

			tmplist = [ d for d in subdirs ]
			for d in tmplist:
				testdir = os.path.join( curdir, d )
				# print( "checking dir ["+d+"]  ["+testdir+"]" )
				if( not is_directory_match(testdir,opts) ):
					# remove this from the list, we don't want to recurse here
					subdirs.remove( d )

			tmplist = [ f for f in files ]
			for f in tmplist:
				testfile = os.path.join( curdir, f )
				if( not is_filename_match(testfile,opts) ):
					# remove this from the list, we don't want to process it
					files.remove( f )

			# create or update all files in this subdir
			for f in files:
				testfile = os.path.join( curdir, f )

				# data = cksum_file( testfile, copy_flag, server_name, opts )
				err = file_func( testfile, func_data, opts )
				if( err != 0 ):
					err_count = err_count + errs
				
			# if present, call progress-bar update
			if( progress_bar is not None ):
				progress_bar.update( len(files) )
		
	return err_count

if __name__ == '__main__':
	print( 'no "main" functionality .. just import the module' )

