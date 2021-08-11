
import os
import sys
from datetime import datetime

import click
from click_repl import register_repl

import mongoengine

from repos_common import init_cli
from repos_model import *
import repos_filerepair as wrk


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
def calcrepair( ctx, rootdir ):
	""" Calculates file-repair info for a set of files/dirs """
	queue = ctx.obj['queue']
	params = {
		'timestamp': datetime.now().isoformat()
	}
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			wrk.calc_filerepair.apply_async( args=(fobj.logical_path, params), queue=queue )

@cli.command()
@click.argument( 'rootdir', type=click.Path() )
@click.pass_context
def wiperepair( ctx, rootdir ):
	""" Wipes the file-repair info from the database """
	queue = ctx.obj['queue']
	params = {
		'timestamp': datetime.now().isoformat()
	}
	with click.progressbar( FileObj.objects( logical_path__startswith=rootdir ) ) as bar:
		for fobj in bar:
			wrk.wipe_filerepair.apply_async( args=(fobj.logical_path, params), queue=queue )

# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	register_repl( cli )
	cli( obj={} )
