# super-simple "repository" for tracking files/file-changes

# for Windows, make a repos.bat file with:
#   @echo off
#   python repos.py %*
# then run, e.g., "repos.bat read data"

import os
import sys
from datetime import datetime

import mongoengine
import click
from click_repl import register_repl

from repos_common import init_cli, to_logical_path, parse_kvtags
from repos_model import FileObj, DirObj, FilesysObj, UserObj


# # # # # # # # # # # # # # # # # # # # # # # # #

def update_file( testfile, params, ctx ):
	err = 0
	create_flag  = params['create_flag']
	server_name  = params['server_name']
	copy_flag    = params['copy_flag']
	copy_tstamp  = params['copy_tstamp']
	kvtags       = params['tags']

	logicalpath = to_logical_path( testfile, ctx )

	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		# if we get here, the file exists
		save_flag = False
		if( copy_flag ):
			save_flag = True
			# TODO: check the checksum here?
			fobj.copies[server_name] = {'last_check':copy_tstamp}
		for k,v in kvtags.items():
			save_flag = True
			fobj.tags[k] = v
		if( save_flag ):
			fobj.save()
	except mongoengine.DoesNotExist as e:
		if( ingest_new ):
			cksum = calc_cksum( filename, ctx )
			fsize = os.path.getsize( filename )
			copies = {}
			if( copy_flag ):
				copies[server_name] = {'last_check':copy_tstamp}
			fobj = FileObj( logical_path=logicalpath, file_size=fsize, checksum=cksum,
					copies=copies, tags=kvtags )
			fobj.save()
		else:
			# we aren't trying to ingest new files, so ignore this
			err = 0
	except Exception as e:
		click.echo( str(e) )
		err = 1

	return err

def delete_file( testfile, params, ctx ):
	err = 0
	logicalpath = to_logical_path( testfile, ctx )
	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		fobj.delete()
	except mongoengine.DoesNotExist as e:
		# should this be an error? we tried to delete something that doesn't exist
		err = 0
	except Exception as e:
		click.echo( str(e) )
		err = 1
	return err

# # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #

@click.group()
@click.option( '--verbose','-v', count=True )
@click.option( '--config','-c','cfg_file', help='read config from file' )
@click.option( '--output','-o','output_file', help='direct the primary output to a file' )
@click.option( '--server','-s','server_name', help='set the server-name' )
@click.option( '--queue','-q','queue_name', help='task-queue to submit backend tasks into' )
@click.pass_context
def cli( ctx, verbose, cfg_file, output_file, server_name, queue_name ):
	"""Simple repository tool """
	init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name )
	if( ctx.obj['server_name'] is None ):
		click.echo( ' * Warning: no server-name info available' )

@cli.command()
@click.pass_context
def config( ctx ):
	click.echo( str(ctx.obj) )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.option( '--copy','-c', 'copy_flag', is_flag=True, help='assign the copy-info for this server' )
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path(exists=True) )
@click.pass_context
def create( ctx, tag_list, copy_flag, filelist ):
	""" Create/add a file to the repos """
	kvtags = parse_kvtags( tag_list )
	(l,server_name) = to_logical_path( filelist[0], ctx, True )
	click.echo( 'server: '+server_name+' : '+ctx.obj['server_name'] )
	for filename in filelist:
		# click.echo( 'looking for file '+filename )
		# check that a file on the filesys exists in db,
		#     or else calculate cksum and store it
		logicalpath = to_logical_path( filename, ctx )
		try:
			fobj = FileObj.objects.get( logical_path=logicalpath )
			click.echo( 'file-obj already exists .. exitting' )
		except mongoengine.DoesNotExist as e:
			# file hasn't been seen before .. cksum it and store it
			cksum = calc_cksum( filename, ctx )
			fsize = os.path.getsize( filename )
			copies = {}
			if( copy_flag ):
				copies[server_name] = {'last_check':datetime.now().isoformat()}
			fobj = FileObj( logical_path=logicalpath, file_size=fsize, checksum=cksum,
					copies=copies, tags=kvtags )
			fobj.save()
			click.echo( 'ok' )
		except Exception as e:
			click.echo( str(e) )

@cli.command()
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path() )
@click.pass_context
def read( ctx, filelist ):
	""" Read/get any repos info known about a file """
	for filename in filelist:
		# click.echo( 'looking for file '+filename )
		logicalpath = to_logical_path( filename, ctx )
		# click.echo( ' .. logical path='+logicalpath )
		try:
			fobj = FileObj.objects.get( logical_path=logicalpath )
			click.echo( fobj )
		except Exception as e:
			# assume DoesNotExist but catch everything for now
			click.echo( 'error '+str(e) )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.option( '--copy','-c', 'copy_flag', is_flag=True, help='assign the copy-info for this server' )
@click.option( '--create','-c', 'create_flag', is_flag=True, help='create if obj does not already exist' )
@click.option( '--recurse','-r', 'recurse_flag', is_flag=True, help='recurse into subdirectories' )
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path() )
@click.pass_context
def update( ctx, tag_list, copy_flag, create_flag, recurse_flag, filelist ):
	""" Update an existing file to the repos """
	(l,server_name) = to_logical_path( filelist[0], ctx, True )
	click.echo( 'server: '+server_name+' : '+ctx.obj['server_name'] )
	kvtags = parse_kvtags( tag_list )
	params = {
		'create_flag': create_flag,
		'server_name': server_name,
		'copy_flag':   copy_flag,
		'copy_tstamp': datetime.now().isoformat(),
		'tags':        kvtags
	}
	errs = 0
	for filename in filelist:
		if( recurse_flag ):
			err = walk_directory_tree( filename, update_file, params, ctx )
		else:
			err = update_file( filename, params, ctx )
		errs = errs + err
	click.echo( 'found '+str(errs)+' errors' )

@cli.command()
@click.option( '--recurse','-r', 'recurse_flag', is_flag=True, help='recurse into subdirectories' )
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path() )
@click.pass_context
def delete( ctx, recurse_flag, filelist ):
	""" Remove file-obj and delete file from file-system """
	errs = 0
	params = {}
	for filename in filelist:
		if( recurse_flag ):
			err = walk_directory_tree( filename, delete_file, params, ctx )
		else:
			err = delete_file( filename, params, ctx )
		errs = errs + err
	click.echo( 'found '+str(errs)+' errors' )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def query( ctx, long_flag, limit, offset, query ):
	""" Query for files that match a pattern (with wildcards) """
	# I like SQL's % signs for wildcards since they don't require quoting in the shell
	if( query is None ):
		the_iterator = FileObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = FileObj.objects( logical_path__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = FileObj.objects( logical_path__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = FileObj.objects( logical_path__startswith = qqq )
	else:
		the_iterator = FileObj.objects( logical_path = query )
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )

	for fobj in the_iterator:
		if( long_flag ):
			click.echo( fobj )
		else:
			click.echo( fobj.logical_path )

@cli.command()
@click.argument( 'query', required=False )
@click.pass_context
def count( ctx, query ):
	""" Query/count for files that match a pattern (with wildcards) """
	# I like SQL's % signs for wildcards since they don't require quoting in the shell
	if( query is None ):
		the_iterator = FileObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = FileObj.objects( logical_path__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = FileObj.objects( logical_path__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = FileObj.objects( logical_path__startswith = qqq )
	else:
		the_iterator = FileObj.objects( logical_path = query )

	click.echo( the_iterator.count() )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.argument( 'query', required=False )
@click.pass_context
def ckquery( ctx, long_flag, query ):
	""" Query for files that match a pattern (with wildcards) """
	# I like SQL's % signs for wildcards since they don't require quoting in the shell
	params = { 'ckquery': query }
	if( long_flag ):
		params['verbose'] = True
	r1 = requests.get( ctx.obj['api_url']+'file/', auth=ctx.obj['api_auth'],
		params=params )
	print( r1.text )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.argument( 'server_name', required=True )
@click.argument( 'root_dir', required=True )
@click.argument( 'is_tape', required=True, type=bool )
@click.argument( 'is_primary', required=False, type=bool )
@click.argument( 'is_readonly', required=False, type=bool )
@click.pass_context
def fsyscreate( ctx, tag_list, server_name, root_dir, is_tape, is_primary, is_readonly ):
	""" Create/add a new filesystem object """
	kvtags = parse_kvtags( tag_list )
	try:
		root_dir = os.path.abspath( root_dir )
		fobj = FilesysObj( server_name=server_name, root_dir=root_dir, is_tape=is_tape,
			is_primary=is_primary, is_readonly=is_readonly, tags=kvtags ).save()
		click.echo( 'ok' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'server_name' )
@click.pass_context
def fsysread( ctx, server_name ):
	""" Read all filesystem info """
	# re-read the known filesystems
	try:
		fobj = FilesysObj.objects.get( server_name=server_name )
		click.echo( fobj )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.option( '--create','-c', 'create_flag', is_flag=True, help='create if obj does not already exist' )
@click.argument( 'server_name', required=True )
@click.argument( 'root_dir', required=False )
@click.argument( 'is_tape', required=False, type=bool )
@click.argument( 'is_primary', required=False, type=bool )
@click.argument( 'is_readonly', required=False, type=bool )
@click.pass_context
def fsysupdate( ctx, tag_list, create_flag, server_name, root_dir, is_tape, is_primary, is_readonly ):
	""" Update a filesystem object """
	kvtags = parse_kvtags( tag_list )
	if( root_dir is not None ):
		root_dir = os.path.abspath( root_dir )
	try:
		fobj = FilesysObj.objects.get( server_name=server_name )
		if( root_dir is not None ):
			fobj.root_dir = root_dir
		if( is_tape is not None ):
			fobj.is_tape = is_tape
		if( is_primary is not None ):
			fobj.is_primary = is_primary
		if( is_readonly is not None ):
			fobj.is_readonly = is_readonly
		for k,v in kvtags.items():
			fobj.tags[k] = v
		fobj.save()
		click.echo( 'ok' )
	except mongoengine.DoesNotExist as e:
		# TODO: add a flag to allow create-if-not-exists?
		if( create_flag ):
			click.echo( 'filesys-obj does not exist .. creating' )
			if( (root_dir is None) or (is_tape is None) ):
				click.echo( 'must define root_dir and is_tape to create new filesys-obj')
			else:
				fobj = FilesysObj( server_name=server_name, root_dir=root_dir, is_tape=is_tape,
					is_primary=is_primary, is_readonly=is_readonly, tags=kvtags ).save()
				click.echo( 'ok' )
		else:
			click.echo( 'filesys-obj does not exist .. exitting' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'server_name', required=True )
@click.pass_context
def fsysdelete( ctx, server_name ):
	""" Delete a filesystem object """
	try:
		fobj = FilesysObj.objects.get( server_name=server_name )
		fobj.delete()
		click.echo( 'ok' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.argument( 'query', required=False )
@click.pass_context
def fsysquery( ctx, long_flag, query ):
	""" Query for filesystems that match a pattern (with wildcards) """
	# NOTE: we already have these stored, but we'll re-read the known filesystems
	if( query is None ):
		the_iterator = FilesysObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = FilesysObj.objects( server_name__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = FilesysObj.objects( server_name__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = FilesysObj.objects( server_name__startswith = qqq )
	else:
		the_iterator = FilesysObj.objects( server_name = query )

	for fobj in the_iterator:
		if( long_flag ):
			click.echo( fobj )
		else:
			click.echo( fobj.server_name )


@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.argument( 'dirname', required=True, type=click.Path() )
@click.argument( 'sponsor', required=True )
@click.argument( 'worker_num', required=True, type=int )
@click.pass_context
def dircreate( ctx, tag_list, dirname, sponsor, worker_num ):
	""" Create/add a directory to the repos """
	kvtags = parse_kvtags( tag_list )
	if( worker_num is None ):
		worker_num = 0
	try:
		dobj = DirObj.objects.get( logical_path=dirname )
		click.echo( 'dir-obj already exists .. exitting' )
	except mongoengine.DoesNotExist as e:
		# dir hasn't been seen before .. cksum it and store it
		# TODO: catch does-not-exist errors for user-obj
		uobj = UserObj.objects.get( username=sponsor )
		dobj = DirObj( logical_path=dirname, sponsor=uobj, worker_num=worker_num, tags=kvtags )
		dobj.save()
		click.echo( 'ok' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path() )
@click.pass_context
def dirread( ctx, filelist ):
	""" Read/get any repos info known about a directory (including ones not on filesys) """
	for filename in filelist:
		# click.echo( 'looking for file '+filename )
		try:
			dobj = DirObj.objects.get( logical_path=filename )
			click.echo( dobj )
		except Exception as e:
			# assume DoesNotExist but catch everything for now
			click.echo( 'error '+str(e) )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.option( '--create','-c', 'create_flag', is_flag=True, help='create if obj does not already exist' )
@click.argument( 'dirname', required=True, type=click.Path() )
@click.argument( 'sponsor' )
@click.argument( 'worker_num', type=int )
@click.pass_context
def dirupdate( ctx, tag_list, create_flag, dirname, sponsor, worker_num ):
	""" Update an existing file to the repos """
	kvtags = parse_kvtags( tag_list )
	try:
		dobj = DirObj.objects.get( logical_path=dirname )
		if( sponsor is not None ):
			# TODO: catch does-not-exist error for user-obj
			uobj = UserObj.objects.get( username=sponsor )
			dobj.sponsor = uobj
		if( worker_num is not None ):
			dobj.worker_num = worker_num
		for k,v in kvtags.items():
			dobj.tags[k] = v
		dobj.save()
		click.echo( 'ok' )
	except mongoengine.DoesNotExist as e:
		# dir hasn't been seen before .. 
		if( create_flag ):
			# TODO: catch does-not-exist errors for user-obj
			uobj = UserObj.objects.get( username=sponsor )
			dobj = DirObj( logical_path=dirname, sponsor=uobj, worker_num=worker_num, tags=kvtags )
			dobj.save()
			click.echo( 'ok' )
		else:
			click.echo( 'dir-obj does not exist .. exitting' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'filelist', nargs=-1, required=True, type=click.Path() )
@click.pass_context
def dirdelete( ctx, filelist ):
	""" Remove dir-obj from the database """
	for filename in filelist:
		try:
			dobj = DirObj.objects.get( logical_path=filename )
			dobj.delete()
			click.echo( 'ok' )
		except Exception as e:
			# assume DoesNotExist but catch everything for now
			click.echo( str(e) )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def dirquery( ctx, long_flag, limit, offset, query ):
	""" Query for directories that match a pattern (with wildcards) """
	# I like SQL's % signs for wildcards since they don't require quoting in the shell
	if( query is None ):
		the_iterator = DirObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = DirObj.objects( logical_path__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = DirObj.objects( logical_path__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = DirObj.objects( logical_path__startswith = qqq )
	else:
		the_iterator = DirObj.objects( logical_path = query )
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )

	for dobj in the_iterator:
		if( long_flag ):
			click.echo( dobj )
		else:
			click.echo( dobj.logical_path )


@cli.command()
@click.argument( 'username', required=True )
@click.argument( 'password', required=True )
@click.argument( 'permissions', required=True, type=int )
@click.pass_context
def usercreate( ctx, username, password, permissions ):
	""" Create/add a directory to the repos """
	try:
		uobj = UserObj.objects.get( username=username )
		click.echo( 'user-obj already exists .. exitting' )
	except mongoengine.DoesNotExist as e:
		uobj = UserObj( username=username, password=password, permissions=permissions )
		uobj.save()
		click.echo( 'ok' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'username', required=True )
@click.pass_context
def userread( ctx, username ):
	""" Read/get any repos info known about a user """
	try:
		uobj = UserObj.objects.get( username=username )
		click.echo( uobj )
	except Exception as e:
		# assume DoesNotExist but catch everything for now
		click.echo( 'error '+str(e) )

@cli.command()
@click.option( '--create','-c', 'create_flag', is_flag=True, help='create if obj does not already exist' )
@click.argument( 'username', required=True )
@click.argument( 'password' )
@click.argument( 'permissions', type=int )
@click.pass_context
def userupdate( ctx, username, password, permissions ):
	""" Update an existing user in the repos """
	try:
		uobj = UserObj.objects.get( username=username )
		if( password is not None ):
			uobj.password = password
		if( permissions is not None ):
			uobj.permissions = permissions
		uobj.save()
		click.echo( 'ok' )
	except mongoengine.DoesNotExist as e:
		# file hasn't been seen before .. cksum it and store it
		if( create_flag ):
			uobj = UserObj( username=username, password=password, permissions=permissions )
			uobj.save()
			click.echo( 'ok' )
		else:
			click.echo( 'user-obj does not exist .. exitting' )
	except Exception as e:
		click.echo( str(e) )

@cli.command()
@click.argument( 'username', required=True )
@click.pass_context
def userdelete( ctx, username ):
	""" Remove user-obj from the database """
	try:
		uobj = UserObj.objects.get( username=username )
		uobj.delete()
		click.echo( 'ok' )
	except Exception as e:
		# assume DoesNotExist but catch everything for now
		click.echo( str(e) )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def userquery( ctx, long_flag, limit, offset, query ):
	""" Query for users that match a pattern (with wildcards) """
	# I like SQL's % signs for wildcards since they don't require quoting in the shell
	if( query is None ):
		the_iterator = UserObj.objects
	elif( (query[0]=='%') and (query[-1]=='%') ):
		qqq = query[1:-1]
		the_iterator = UserObj.objects( username__contains = qqq )
	elif( query[0] == '%' ):
		qqq = query[1:]
		the_iterator = UserObj.objects( username__endswith = qqq )
	elif( query[-1] == '%' ):
		qqq = query[:-1]
		the_iterator = UserObj.objects( username__startswith = qqq )
	else:
		the_iterator = UserObj.objects( username = query )
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )

	for uobj in the_iterator:
		if( long_flag ):
			click.echo( uobj )
		else:
			click.echo( uobj.username )


@cli.command()
@click.option( '--yes','yes_flag', is_flag=True, prompt='Confirm deletion' )
@click.pass_context
def wipe( ctx, yes_flag ):
	""" Wipe all files from database """
	if( yes_flag ):
		for fobj in FileObj.objects():
			fobj.delete()
		for fobj in FilesysObj.objects():
			fobj.delete()
		for dobj in DirObj.objects():
			dobj.delete()
		# for uobj in UserObj.objects():
		# 	uobj.delete()
	else:
		click.echo( 'use --yes flag to confirm the deletion of all files' )


#
# some simpler/debugging scripts
#

@cli.command()
@click.argument( 'filelist', nargs=-1, required=True )
@click.pass_context
def logpath( ctx, filelist ):
	""" Show logical path for local file """
	for filename in filelist:
		# click.echo( 'looking for file '+filename )
		(logicalpath,server_name) = to_logical_path( filename, ctx, True )
		print( server_name + '::' + logicalpath )


# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
