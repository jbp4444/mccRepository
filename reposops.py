# super-simple "repository" for tracking files/file-changes

# for Windows, make a repos.bat file with:
#   @echo off
#   python repos.py %*
# then run, e.g., "repos.bat read data"

import os
from datetime import datetime
from glob import glob

import mongoengine
from mongoengine.queryset.visitor import Q, QCombination
# from mongoengine import Q, QCombination

import click
from click_repl import register_repl

from repos_common import init_cli, calc_cksum
from repos_model import FileObj
import repos_worksubs as wrk

# # # # # # # # # # # # # # # # # # # # # # # # #

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
	"""Simple repository tool
			e.g. reposops --any image_type=jpeg image_type=gif  ... show jpeg or gif images
	"""
	init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def pending( ctx, long_flag, limit, offset, query ):
	""" Query for files with pending information (that match a pattern; with wildcards) """
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

	outlog = ctx.obj['output']
	outlog.info( 'starting pending scan' )

	count = 0
	for fobj in the_iterator:
		cur_val = fobj.checksum['sha1']
		if( cur_val == '__PENDING__' ):
			if( long_flag ):
				outlog.info( str(fobj) )
			count = count + 1

	outlog.info( 'count='+str(count) )
	click.echo( 'count='+str(count) )

@cli.command()
@click.option( '--move','move_flag', is_flag=True, help='move duplicate files to temp dir' )
@click.option( '--count','count_flag', is_flag=True, help='report count of duplicate files' )
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def dedupe( ctx, move_flag, count_flag, long_flag, limit, offset, query ):
	""" Query for files that match a pattern (with wildcards) """
	queue = ctx.obj['queue']
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

	count = 0
	count_fnmatch = 0

	outlog = ctx.obj['output']
	outlog.info( 'starting dedupe scan' )

	last_fobj = 0
	last_val = 0
	for fobj in the_iterator.order_by('checksum.sha1'):
		cur_val = fobj.checksum['sha1']
		if( last_val == cur_val ):
			if( cur_val != '__PENDING__' ):
				if( move_flag ):
					outlog.info( fobj.logical_path + ' :: ' + last_fobj.logical_path )
					wrk.dedupe_file.apply_async( args=(last_fobj.logical_path,fobj.logical_path), queue=queue )

				elif( count_flag ):
					count = count + 1
					# is there a filename match?
					fn1 = os.path.basename( fobj.logical_path )
					fn2 = os.path.basename( last_fobj.logical_path )
					if( fn1 == fn2 ):
						count_fnmatch = count_fnmatch + 1
					
				elif( long_flag ):
					txt1 = str(fobj)
					txt2 = str(last_fobj)
					outlog.info( txt1 + ' :: ' + txt2 )
				
				else:
					outlog.info( fobj.logical_path + ' :: ' + last_fobj.logical_path )
		last_fobj = fobj
		last_val = cur_val

	outlog.info( 'found %d duplicate files, including %d filename matches'%(count,count_fnmatch) )
	if( count_flag ):
		click.echo( 'found %d duplicate files, including %d filename matches'%(count,count_fnmatch) )


@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def filestats( ctx, rootdir ):
	""" Calculates basic file stats from the database """
	sz_sum = 0.0
	sz_sum2 = 0.0
	count = 0

	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			fsize   = fobj.file_size
			sz_sum  = sz_sum + fsize
			sz_sum2 = sz_sum + fsize*fsize
			count   = count + 1

	click.echo( '%d files, %d bytes total,   avg file size %.2f bytes'%(count,sz_sum,sz_sum/count) )

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
@click.argument( 'message', default='dummytask', type=click.STRING )
@click.pass_context
def dummytask( ctx, message ):
	queue = ctx.obj['queue']
	wrk.dummy_task.apply_async( args=(message,), queue=queue )


@cli.command()
@click.argument( 'filepath_wc', type=click.Path() )
@click.pass_context
def ckdel( ctx, filepath_wc ):
	""" Checks if a file is already in the system and deletes it if it is """
	
	for filepath in glob(filepath_wc):
		click.echo( 'processing file [%s]'%(filepath) )
		if( os.path.isfile(filepath) ):
			filesize = os.path.getsize( filepath )
			cksum = calc_cksum( filepath )
			click.echo( '   size=%d  cksum=%s'%(filesize,cksum) )

			match = FileObj.objects( checksum__sha1=cksum ).first()
			if( match is None ):
				click.echo( '   no cksum match found!' )
			elif( filesize == match.file_size ):
				# click.echo( '   cksum+filesize match found with: '+str(match) )
				click.echo( '   cksum+filesize match .. deleting file' )
				os.unlink( filepath )
			else:
				click.echo( '   cksum match but file size mismatch found' )
		else:
			click.echo( '   not a file' )


# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
