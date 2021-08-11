# super-simple "repository" for tracking files/file-changes

# for Windows, make a repos.bat file with:
#   @echo off
#   python repos.py %*
# then run, e.g., "repos.bat read data"

import os
import sys
from datetime import datetime

import mongoengine
from mongoengine.queryset.visitor import Q, QCombination
# from mongoengine import Q, QCombination

import click
from click_repl import register_repl

from repos_common import init_cli, parse_kvtags, to_logical_path, walk_directory_tree
from repos_model import FileObj, FilesysObj
import repos_imgsubs as wrkimg
import repos_musicsubs as wrkmus

# # # # # # # # # # # # # # # # # # # # # # # # #

def walk_and_tag( testfile, params, ctx ):
	err = 0

	kvtags = params['tags']
	logicalpath = to_logical_path( testfile, ctx )

	to_process = False
	fobj = None
	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		to_process = True
	except mongoengine.DoesNotExist as e:
		to_process = False
	except Exception as e:
		err = 1
		to_process = False

	if( to_process ):
		for k,v in kvtags.items():
			fobj.tags[k] = v
		fobj.save()
		# TODO: catch errors with save()

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
	"""Simple repository tool
			e.g. tagquery --any image_type=jpeg image_type=gif  ... show jpeg or gif images
	"""
	init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name )

@cli.command()
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'tag_query',nargs=-1 )  #help='key=val tag(s) to query for (multiple args allowed)' )
@click.pass_context
def tagfullquery( ctx, long_flag, limit, offset, tag_query ):
	""" Query for files that match a pattern (with wildcards)
			e.g. tagquery image_type=jpeg   ... show all jpeg images
	"""
	kvtags = parse_kvtags( tag_query )
	for kk,v in kvtags.items():
		if( kk[0] == '!' ):
			k = kk[1:]
		else:
			k = kk
		print( 'k='+kk+'/'+k+'  v='+str(v) )

	# I like SQL's % signs for wildcards since they don't require quoting in the shell

	the_iterator = FileObj.objects
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )

	for fobj in the_iterator:
		tags = fobj.tags

		match = True
		for kk,v in kvtags.items():
			if( kk[0] == '!' ):
				k = kk[1:]
			else:
				k = kk
			this_match = False
			if( k in tags ):
				if( (v[0]=='%') and (v[-1]=='%') ):
					# two wildcards on this key=val pair
					qqq = v[1:-1]
					if( qqq in tags[k] ):
						this_match = True
				elif( v[0] == '%' ):
					# opening wildcard on this key=val pair
					qqq = v[1:]
					if( tags[k][-n:] == qqq ):
						this_match = True
				elif( v[-1] == '%' ):
					# ending wildcard on this key=val pair
					qqq = v[:-1]
					if( tags[k][:n] == qqq ):
						this_match = True
				else:
					# no wildcards on this key=val pair .. exact match required
					if( tags[k] == v ):
						this_match = True
			
			if( kk[0] == '!' ):
				match = match and (not this_match)
			else:
				match = match and this_match

			if( match == False ):
				break

		if( match ):
			if( long_flag ):
				click.echo( fobj )
			else:
				click.echo( fobj.logical_path )

@cli.command()
@click.option( '--any', 'any_flag', is_flag=True, help='return file-objects which match any of the given key-val pairs, default=False (logical or)' )
@click.option( '--all', 'all_flag', is_flag=True, help='return file-objects which match all of the given key-val pairs, default=True (logical and)' )
@click.option( '--long','long_flag', is_flag=True, help='"long" format output (all data)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'tag_query',nargs=-1 )  #help='key=val tag(s) to query for (multiple args allowed)' )
@click.pass_context
def tagquery( ctx, any_flag, all_flag, long_flag, limit, offset, tag_query ):
	""" Query for files that match a pattern (with wildcards)
			e.g. tagquery image_type=jpeg   ... show all jpeg images
	"""
	kvtags = parse_kvtags( tag_query )
	for k,v in kvtags.items():
		print( 'k='+k+'  v='+str(v) )

	# local var for logical op across all key-val pairs
	if( any_flag ):
		all_flag = False
	elif( all_flag == False ):
		all_flag = True   # default if no args given

	# I like SQL's % signs for wildcards since they don't require quoting in the shell

	# start with basic iterator across all objects
	the_iterator = FileObj.objects
	# : now add in any limit/skip params
	if( offset is not None ):
		the_iterator = the_iterator.skip( int(offset) )
	if( limit is not None ):
		the_iterator = the_iterator.limit( int(limit) )

	# : now add in kv params .. but just to trim down the query, not to filter exact entries
	Qfilters = []
	for k,v in kvtags.items():
		# flt = {'tags__exists': k }
		flt = { 'tags__%s'%(k): v }
		Qfilters.append( Q( **flt ) )
	if( all_flag ):
		click.echo( 'querying for all kv-pairs' )
		Qquery = QCombination( QCombination.AND, Qfilters )
	else:
		click.echo( 'querying for any kv-pairs' )
		Qquery = QCombination( QCombination.OR, Qfilters )
	the_iterator = the_iterator.filter( Qquery )

	for fobj in the_iterator:
		if( long_flag ):
			click.echo( fobj )
		else:
			click.echo( fobj.logical_path )

@cli.command()
@click.argument( 'tag_query',nargs=-1 )  #help='key=val tag(s) to query for (multiple args allowed)' )
@click.pass_context
def cleantags( ctx, tag_query ):
	""" Clean up DB - remove any files that match a tag-pattern (with wildcards)
			e.g. run ./nightly.bat walktag -t mark=true c:/path/to/data
			then run ./cleandb.bat cleandb !mark=true
	"""
	kvtags = parse_kvtags( tag_query )
	for kk,v in kvtags.items():
		if( kk[0] == '!' ):
			k = kk[1:]
		else:
			k = kk
		print( 'k='+kk+'/'+k+'  v='+str(v) )

	# I like SQL's % signs for wildcards since they don't require quoting in the shell

	the_iterator = FileObj.objects

	for fobj in the_iterator:
		tags = fobj.tags

		match = True
		for kk,v in kvtags.items():
			if( kk[0] == '!' ):
				k = kk[1:]
			else:
				k = kk
			this_match = False
			if( k in tags ):
				if( (v[0]=='%') and (v[-1]=='%') ):
					# two wildcards on this key=val pair
					qqq = v[1:-1]
					if( qqq in tags[k] ):
						this_match = True
				elif( v[0] == '%' ):
					# opening wildcard on this key=val pair
					qqq = v[1:]
					if( tags[k][-n:] == qqq ):
						this_match = True
				elif( v[-1] == '%' ):
					# ending wildcard on this key=val pair
					qqq = v[:-1]
					if( tags[k][:n] == qqq ):
						this_match = True
				else:
					# no wildcards on this key=val pair .. exact match required
					if( tags[k] == v ):
						this_match = True
			
			if( kk[0] == '!' ):
				match = match and (not this_match)
			else:
				match = match and this_match

			if( match == False ):
				break

		if( match ):
			fobj.delete()
			click.echo( fobj.logical_path )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.option( '--limit','-l','limit', type=int, help='limit number of returned items' )
@click.option( '--offset','-o','offset', type=int, help='offset to start returning items from' )
@click.argument( 'query', required=False )
@click.pass_context
def tagfiles( ctx, tag_list, limit, offset, query ):
	""" Tag all files that match a filename pattern (with wildcards) """
	kvtags = parse_kvtags( tag_list )

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
		for k,v in kvtags.items():
			fobj.tags[k] = v
		fobj.save()

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def tagimages( ctx, rootdir ):
	""" Tag any images with basic metadata [backend tasks] """
	queue = ctx.obj['queue']
	params = {
		'timestamp': datetime.now().isoformat(),
		'server_name': ctx.obj['server_name']
	}
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			wrkimg.tag_image_data.apply_async( args=(fobj.logical_path, params), queue=queue )

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def tagmusic( ctx, rootdir ):
	""" Tag any images with basic metadata [backend tasks] """
	queue = ctx.obj['queue']
	params = {
		'timestamp': datetime.now().isoformat(),
		'server_name': ctx.obj['server_name']
	}
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			wrkmus.tag_music_data.apply_async( args=(fobj.logical_path, params), queue=queue )

@cli.command()
@click.option( '--tag','-t', 'tag_list', multiple=True, help='key=val tag(s) to assign (multiple args allowed)' )
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def walktag( ctx, tag_list, rootdir ):
	""" Walk a filesystem and tag any files found with key=value tags """
	kvtags = parse_kvtags( tag_list )
	if( '~' in rootdir ):
		rootdir = os.path.expanduser( rootdir )
	# TODO: better way to determine server were running on?
	(logicalpath,server_name) = to_logical_path( rootdir, ctx, True )
	if( server_name is not None ):
		fsys = FilesysObj.objects.get( server_name=server_name )
		# if( fsys.is_primary is False ):
		# 	click.echo( '* ERROR: must run "walktag" command on primary file system' )
		# 	return
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
			'tags':         kvtags
		}
		errs = walk_directory_tree( rootdir, walk_and_tag, params, ctx )

	click.echo( 'found '+str(errs)+' errors' )

@cli.command()
@click.pass_context
def tagbasename( ctx ):
	""" Tag all files with their file basename (os.path.basename) """

	for fobj in FileObj.objects:
		fobj.tags['basename'] = os.path.basename( fobj.logical_path )
		fobj.save()

@cli.command()
@click.option( '-o','--output','outfile', type=click.Path(), help='output file' )
@click.pass_context
def tagbasededupe( ctx, outfile ):
	""" Dedupe all files based on their file basename (os.path.basename) """
	skiplist = [ 'Folder.jpg', 'folder.jpg', 'Cover.jpg', 'cover.jpg', 'AlbumArtSmall.jpg' ]

	fp_out = sys.stdout
	if( outfile is not None ):
		fp_out = open( outfile, 'w' )

	for fobj in FileObj.objects:
		basename = os.path.basename( fobj.logical_path )

		if( 'Track' in basename ):
			pass
		elif( 'track' in basename ):
			pass
		else:
			# find any files with same basename-tag
			for kobj in FileObj.objects( tags__basename=basename ):
				if( fobj != kobj ):
					if( basename in skiplist ):
						pass
					else:
						fp_out.write( 'dupe found: '+str(fobj)+'  ::  '+str(kobj)+'\n' )

	fp_out.close()


# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
