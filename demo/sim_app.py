#
# simulate an app that is depositing files onto the primary file system
#


import os
import sys
import time
from datetime import datetime
import random

# for x in sys.path:
# 	print( x )

import mongoengine
import click
from click_repl import register_repl

from repos_common import init_cli, to_logical_path, parse_kvtags
from repos_model import FileObj, DirObj, FilesysObj


# # # # # # # # # # # # # # # # # # # # # # # # #

chars62 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

def make_file( fsys, filename ):
	fname = os.path.abspath( fsys.root_dir + '\\' + filename )
	with open(fname,'w') as fp:
		fsize = random.randint(100,150)
		fp.write( ''.join( random.choices( chars62, k=fsize ) ) )
	return

# # # # # # # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # #

@click.group()
@click.option( '--verbose','-v', count=True )
@click.option( '--config','-c','cfg_file', help='read config from file' )
@click.option( '--output','-o','output_file', help='direct the primary output to a file' )
@click.pass_context
def cli( ctx, verbose, cfg_file, output_file ):
	""" Simple repository tool """
	init_cli( ctx, verbose, cfg_file, output_file, None, None )

# # # # # # # # # # # # # # # # # # # # # # # # #

@cli.command()
@click.option( '-i','interval', type=int, default=33, help='interval for new file creation' )
@click.option( '-s','start_val', type=int, default=0, help='starting counter for filenames' )
@click.option( '-n','num_files', type=int, default=10, help='number of new files to create' )
@click.pass_context
def runapp( ctx, interval, start_val, num_files ):
	random.seed()
	fsys_list = ctx.obj['fsys_list']

	fsys_primary = None
	for fobj in fsys_list:
		if( fobj.is_primary ):
			fsys_primary = fobj
			break
	
	click.echo( fsys_primary )

	ctr = 0

	while( True ):
		fname = 'file%04d'%(ctr + start_val)
		click.echo( fname )

		make_file( fsys_primary, fname )

		time.sleep( interval )

		ctr = ctr + 1
		if( ctr > num_files ):
			break


# # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	cli( obj={} )

