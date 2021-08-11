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
from repos_model import FileObj, DirObj, FilesysObj


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
@click.option( '--yes','yes_flag', is_flag=True, prompt='Confirm deletion' )
@click.pass_context
def wipeall( ctx, yes_flag ):
	""" Wipe all files, file-systems, directories, and users from database """
	if( yes_flag ):
		for fobj in FileObj.objects():
			fobj.delete()
		for fobj in FilesysObj.objects():
			fobj.delete()
		for dobj in DirObj.objects():
			dobj.delete()
		for uobj in UserObj.objects():
			uobj.delete()
	else:
		click.echo( 'use --yes flag to confirm the deletion of all files' )

@cli.command()
@click.option( '--yes','yes_flag', is_flag=True, prompt='Confirm deletion' )
@click.pass_context
def wipefsys( ctx, yes_flag ):
	""" Wipe all file-copies for a given file-system (from database) """
	server_name = ctx.obj['server_name']

	if( yes_flag ):
		# go thru all files
		with click.progressbar( FileObj.objects() ) as bar:
			for fobj in bar:
				del fobj.copies[server_name]
				fobj.save()

		# now delete the file-sys obj itself
		try:
			fobj = FilesysObj.objects.get( server_name=server_name )
			fobj.delete()
			click.echo( 'ok' )
		except Exception as e:
			click.echo( str(e) )

	else:
		click.echo( 'use --yes flag to confirm the deletion of all files' )


# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
