
import os
import sys

import mongoengine

from repos_worker import celapp, fsys_list, fsys_primary
from repos_model import *
from repos_common import *

import filerepair

@celapp.task
def calc_filerepair( logicalpath, params ):
	print( 'calc filerepair for '+logicalpath )

	fobj = FileObj.objects.get( logical_path=logicalpath )

	# if( 'filerepair' in fobj.tags ):
	#	pass

	real_filename = to_local_path( logicalpath, fsys_primary['server_name'] )

	opts = filerepair.utils.DefaultOpts()

	rtn = filerepair.create_from_file( real_filename, None, opts )

	if( type(rtn) is int ):
		print( '* Error: could not calc filerepair info %d'%(rtn) )
	else:
		fobj.tags['filerepair'] = rtn
		fobj.save()

	return 0

# TODO: NOT WORKING!!
@celapp.task
def verify_filerepair( logicalpath, params ):
	print( 'verify filerepair for '+logicalpath )

	fobj = FileObj.objects.get( logical_path=logicalpath )

	# if( 'filerepair' in fobj.tags ):
	#	pass

	real_filename = to_local_path( logicalpath, fsys_primary['server_name'] )

	opts = filerepair.utils.DefaultOpts()

	rtn = filerepair.create_from_file( real_filename, None, opts )

	if( type(rtn) is int ):
		print( '* Error: could not verify filerepair info %d'%(rtn) )
	else:
		# fobj.tags['filerepair'] = rtn
		# fobj.save()
		# TODO: verify that newly calculated data == data stored in db

	return 0

@celapp.task
def wipe_filerepair( logicalpath, params ):
	print( 'wipe filerepair for '+logicalpath )

	fobj = FileObj.objects.get( logical_path=logicalpath )
	if( 'filerepair' in fobj.tags ):
		del fobj.tags['filerepair']
		fobj.save()

	return 0
