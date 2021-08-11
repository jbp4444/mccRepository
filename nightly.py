# super-simple "repository" for tracking files/file-changes
#
# examples of more "complex" scripts to run ...
#

# for Windows, make a reposscan.bat file with:
#   @echo off
#   python reposscan.py %*
# then run, e.g., "reposscan.bat scan data"

import os
from datetime import datetime

import mongoengine
import click
from click_repl import register_repl

from repos_common import init_cli, to_logical_path, to_local_path, walk_directory_tree, parse_kvtags
from repos_model import FileObj, FilesysObj
import repos_worksubs as wrk

# # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #

def scan_for_new_files( testfile, params, ctx ):
	err = 0

	queue = params['queue']
	server_name = params['server_name']
	logicalpath = to_logical_path( testfile, ctx )

	to_process = False
	fobj = None
	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		# if we get here, the file exists in db
		if( server_name not in fobj.copies ):
			to_process = True
	except mongoengine.DoesNotExist as e:
		to_process = True
	except Exception as e:
		err = 1
		to_process = False

	if( to_process ):
		if( fobj is None ):
			# create a sentinel file-obj so that future scans will "see" it
			copies = {
				server_name: {
					'last_cksum': '__PENDING__'
				}
			}
			checksum = { params['cksum_algo']: '__PENDING__' }
			fobj = FileObj( logical_path=logicalpath, file_size=-1, checksum=checksum, copies=copies )
		else:
			fobj.copies[server_name] = { 'last_cksum': '__PENDING__' }
		fobj.save()
		# TODO: catch errors with save()
		wrk.ingest_new_file.apply_async( args=(logicalpath, params), queue=queue )
		# TODO: catch errors with new-task creation

	return err


# # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #

@click.group()
@click.option( '--verbose','-v', count=True )
@click.option( '--config','-c','cfg_file', help='read config from file' )
@click.option( '--output','-o','output_file', help='direct the primary output to a file' )
@click.option( '--server','-s','server_name', help='set server-name' )
@click.option( '--queue','-q','queue_name', help='task-queue to submit backend tasks into' )
@click.pass_context
def cli( ctx, verbose, cfg_file, output_file, server_name, queue_name ):
	"""Simple repository tool """
	init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name )

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def scan( ctx, rootdir ):
	""" Scan/walk a filesystem and ingest any files [backend tasks] """
	# if( '~' in rootdir ):
	# 	rootdir = os.path.expanduser( rootdir )
	# TODO: better way to determine server were running on?
	(logicalpath,server_name) = to_logical_path( rootdir, ctx, True )
	if( server_name is not None ):
		fsys = FilesysObj.objects.get( server_name=server_name )
		if( fsys.is_primary is False ):
			click.echo( '* ERROR: must run "scan" command on primary file system' )
			return
		click.echo( 'running on server '+server_name+' ...' )
	else:
		click.echo( '* ERROR: cannot find server_name' )
		return
	
	logger = ctx.obj['logger']
	logger.info( 'starting scan of (%s,%s)'%(server_name,rootdir) )

	# count num files we have to look at in directory-walk
	p = {
		'total_count': 0
	}
	def count_files( filename, params, opts ):
		params['total_count'] = params['total_count'] + 1
		return 0
	walk_directory_tree( rootdir, count_files, p, ctx )
	click.echo( 'found %d files to process'%(p['total_count']) )

	# now do the actual walk of the dir-tree
	with click.progressbar( length=p['total_count'] ) as bar:
		ctx.obj['progress_bar'] = bar

		params = {
			'server_name':  server_name,
			'timestamp':    datetime.now().isoformat(),
			'cksum_algo':   ctx.obj['cksum_algo'],
			'queue':        ctx.obj['queue']
		}
		errs = walk_directory_tree( rootdir, scan_for_new_files, params, ctx )

	click.echo( 'found '+str(errs)+' errors' )

@cli.command()
@click.argument( 'logicalpath', type=click.Path() )
@click.pass_context
def sync( ctx, logicalpath ):
	""" Push known files from primary to secondary file-system [backend tasks] """
	# Nightly sync of primary to secondary filesystems
	# : this ONLY sync's file, does NOT do a full cksum
	# : assume this runs nightly on all non-primary file-sys

	timestamp    = datetime.now().isoformat()
	fsys_list    = ctx.obj['fsys_list']
	fsys_primary = ctx.obj['fsys_primary']
	queue        = ctx.obj['queue']

	outlog = ctx.obj['output']
	outlog.info( 'starting sync of %s'%(logicalpath) )

	with click.progressbar( FileObj.objects( logical_path__startswith=logicalpath ) ) as bar:
		# TODO: check for errors (shouldn't be any, but maybe file was deleted while we waited in queue)
		for fobj in bar:
			copies = fobj.copies

			# push this file to all known file-systems
			# TODO: allow for specific fsys-to-fsys pushes
			for fs in fsys_list:
				if( (fs != fsys_primary) and (fs.is_readonly is False) ):
					server_name = fs.server_name
					# convert to localpath so we can cksum it
					localpath = to_local_path( fobj.logical_path, server_name )

					need_to_copy = False
					if( server_name in copies ):
						# this file-sys has seen this file before
						# : does it still exist?
						if( os.path.isfile(localpath) ):
							# low-cost verification(s) that it is the same file?
							# : checksums will be done in another task
							fsize = os.path.getsize( localpath )
							if( fsize != fobj.file_size ):
								outlog.warning( 'File-size has changed for %s (%d to %d)'%(fobj.logical_path,fsize,fobj.file_size) )
								need_to_copy = True
						else:
							outlog.warning( 'File not found on fsys anymore (%s,%s)'%(fobj.logical_path,server_name) )
							need_to_copy = True
					else:
						# file never seen before
						outlog.info( 'Copying file to fsys (%s,%s)'%(fobj.logical_path,server_name) )
						need_to_copy = True

					if( need_to_copy ):
						# file does not exist, or something wrong with it
						outlog.info( 'Need to copy file: '+fobj.logical_path+' to '+server_name )
						
						# flag this file-obj to indicate that we know it needs a copy/copy is pending
						fobj.copies[server_name] = { 'pending': True }
						fobj.save()

						wrk.copy_file.apply_async( args=(fobj.logical_path, fsys_primary['server_name'], server_name), queue=queue )

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def verify( ctx, rootdir ):
	""" Verify known files against what's actually on disk [backend tasks] """
	queue        = ctx.obj['queue']
	params = {
		'timestamp':   datetime.now().isoformat(),
		'server_name': ctx.obj['server_name'],
		'cksum_algo':  ctx.obj['cksum_algo']
	}

	logger = ctx.obj['logger']
	logger.info( 'starting verify of %s'%(rootdir) )

	# for fobj in FileObj.objects():
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			wrk.verify_existing_file.apply_async( args=(fobj.logical_path, params), queue=queue )

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def checkpolicy( ctx, rootdir ):
	""" Verify files in database against what's on disk """
	err_count = 0

	outlog = ctx.obj['output']
	outlog.info( 'starting check-policy on %s'%(rootdir) )

	fsys = {}
	for f in ctx.obj['fsys_list']:
		fsys[ f['server_name'] ] = f
		outlog.info( 'filesystem:'+str(f) )

	# for fobj in FileObj.objects( logical_path__startswith=rootdir ):
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir )) as bar:
		for fobj in bar:
			err = 0
			if( fobj.file_size < 0 ):
				outlog.error( 'File size error' )
				err = err - 1
			if( fobj.checksum == '' ):
				outlog.error( 'Checksum error' )
				err = err - 2

			disk_copies = 0
			tape_copies = 0
			for sname,val in fobj.copies.items():
				srv = fsys[sname]
				if( srv.is_tape ):
					for k,v in val.items():
						if( k[:4] == 'copy' ):
							tape_copies = tape_copies + 1
				else:
					disk_copies = disk_copies + 1

			if( disk_copies < 2 ):
				outlog.error( 'Not enough disk copies: %d %s'%(disk_copies,fobj.logical_path) )
				err = err - 4
			if( tape_copies < 2 ):
				outlog.error( 'Not enough tape copies: %d %s'%(tape_copies,fobj.logical_path) )
				err = err - 8

			# TODO: check that cksum time-stamps are not too old

			if( err < 0 ):
				err_count = err_count + 1
	
	outlog.info( 'err count=%d'%(err_count) )
	click.echo( 'err count=%d'%(err_count) )

@cli.command()
@click.option( '--force','-f','force_flag', is_flag=True, required=False, help='force re-attachment even if already present' )
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def attachdirs( ctx, force_flag, rootdir ):
	""" For all known files, attach them to (known) directories """
	# for fobj in FileObj.objects():
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			# find path components for this file
			(dname,fname) = os.path.split( fobj.logical_path )

			to_process = False
			if( fobj.directory is None ):
				to_process = True
			elif( force_flag ):
				to_process = True

			if( to_process ):
				# do any dirs match?
				# TODO: there must be a better way to do this
				best_dobj = None
				# best_len  = 0
				# for dobj in DirObj.objects():
				# 	ln = len(dobj.logical_path)
				# 	if( fobj.logical_path[:ln] == dobj.logical_path ):
				# 		# the 'best' match will be the longest one (most subdirs matched)
				# 		if( ln > best_len ):
				# 			best_dobj = dobj
				while( dname != '' ):
					dobj = DirObj.objects.get( logical_path=dname )
					if( dobj is not None ):
						best_dobj = dobj
						dname = ''
						break
					else:
						(dname,fname) = os.path.split( dname )

				if( best_dobj is None ):
					logger.warning( '* Warning: could not find dir match for file '+logicalpath )
				else:
					fobj.directory = best_dobj
					fobj.save()

# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
