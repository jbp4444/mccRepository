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

from repos_common import init_cli
from repos_model import FileObj

# # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #

# fudge it .. tape a has all filename that start with a
all_tapes = []
tapedata = {}
# ... really num tapes per copy ...
num_tapes = 8

# NOTE: by design, python's hash func is seeded randomly every time it is run
#       so we'll use zlib.crc32
import zlib
def copy1hash( filename ):
	# h = hash( 'copy1:'+filename ) % num_tapes
	h = zlib.crc32( bytes('copy1:'+filename,'utf-8') ) % num_tapes
	return 'DUL1%02d'%(h)
def copy2hash( filename ):
	# h = hash( 'copy2:'+filename ) % num_tapes
	h = zlib.crc32( bytes('copy2:'+filename,'utf-8') ) % num_tapes
	return 'DUL2%02d'%(h)

def process_tape( tape_id, params, ctx ):
	server_name = params['server_name']
	timestamp   = params['timestamp']
	logger      = ctx.obj['logger']

	verbose = ctx.obj['verbose']

	# NOTE: copy_num should be an int, but we only use it as a string
	copy_num    = tape_id[3]
	logger.info( 'parsing tape %s, copynum=%s'%(tape_id,copy_num) )

	# TODO: run "external" command to get files on this tape
	# : e.g. fsmedinfo -l tape_id
	filelist = tapedata[tape_id]

	# for each file, mark it as a good copy
	for f in filelist:
		# TODO: check for errors
		fobj = FileObj.objects.get( logical_path=f )
		copies = fobj.copies
		if( server_name not in copies ):
			# this tape is already marked, update it
			# logger.info( 'file not previously marked: '+f )
			cdata = {}
		else:
			cdata = fobj.copies[server_name]
		# add tape/copy data to existing info
		cdata['copy'+copy_num] = {
			'tape_id': tape_id,
			'last_edlm': timestamp
		}
		fobj.copies[server_name] = cdata
		fobj.save()
		logger.info( 'EDLM check OK for copy=%s of %s'%(copy_num,f) )

		if( verbose > 0 ):
			click.echo( 'EDLM check OK for copy=%s of %s'%(copy_num,f) )

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
	""" Simple repository tool """
	init_cli( ctx, verbose, cfg_file, output_file, server_name, queue_name )

	for tid in range(num_tapes):
		tape = 'DUL1%02d'%(tid)
		all_tapes.append( tape )
		tapedata[tape] = []

		tape = 'DUL2%02d'%(tid)
		all_tapes.append( tape )
		tapedata[tape] = []

	# fudge the tape-to-file mapping
	for fobj in FileObj.objects():
		tape = copy1hash( fobj.logical_path )
		tapedata[tape].append( fobj.logical_path )
		tape = copy2hash( fobj.logical_path )
		tapedata[tape].append( fobj.logical_path )

	# check that the hash is stable (unlike python's hash func)
	# print( copy1hash('aaa') )
	# print( copy2hash('aaa') )

# the idea is that a given tape will have been scanned by some out-of-band process
# (native to the tape library), and all we know is "tape 456 is good" .. so we now
# need to conver that to a set of files that are also now known-good
# : on Quantum/Artico .. fsmedinfo can list all files on a given tape
@cli.command()
@click.argument( 'tape_ids', nargs=-1, required=True )
@click.pass_context
def tapescan( ctx, tape_ids ):
	""" Scan tape(s) and mark copies in database """

	params = {
		'timestamp': datetime.now().isoformat(),
		'server_name': ctx.obj['fsys_tape']['server_name']
	}

	for tape_id in tape_ids:
		process_tape( tape_id, params, ctx )

# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
