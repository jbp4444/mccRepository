#!/usr/bin/python

import os
import sys
import hashlib
import requests

# opts = utils.DefaultOpts()

opts = {}
opts['verbose'] = 1

# basedir for the file tree
basedir = 'data'

argc = len(sys.argv)

if( argc > 1 ):
	basedir = sys.argv[1]

# # # # # # # # # # # # # # # # # # # # #

def calc_fullpath( fname ):
	rtn = basedir + '/'
	for i in range(num_levels):
		rtn = rtn + fname[i] + '/'
	rtn = rtn + fname
	return rtn

def strip_frontdirs( fname ):
	lst = string.split( fname, '/' )
	#print( "fname=",fname,lst )
	return '/'.join( lst[num_levels+1:] )

def calc_cksum( fname ):
	h_md5 = hashlib.md5()
	with open( fname,'rb') as f:
		for chunk in iter(lambda: f.read(4096), b""):
			h_md5.update(chunk)
	return h_md5.hexdigest()

# # # # # # # # # # # # # # # # # # # # #

def get_fsys_list():
	r = requests.get( 'http://localhost:5000/fsys/',
		auth=('oona','oonaoona') )
	print( 'GET::'+str(r.status_code)+'::'+r.text )

	rj = r.json()

	rtn = { 'info':'undefined error', 'status':'error' }

	if( rj['status'] == 'ok' ):
		rtn = rj

	return rtn

def get_file( fname ):
	r = requests.get( 'http://localhost:5000/file/' + fname,
		auth=('oona','oonaoona') )
	print( 'GET::'+str(r.status_code)+'::'+r.text )

	rj = r.json()

	rtn = { 'info':'undefined error', 'status':'error' }

	if( rj['status'] == 'ok' ):
		rtn = rj

	return rtn

def missing_file( infile, fobj ):
	print( "    missing file at "+infile )
	return 1

def new_file( infile ):
	print( "    new file at "+infile )
	return 0

# # # # # # # # # # # # # # # # # # # # #

def process_file( fname, _opts ):
	print( "processing file "+fname )
	meta = get_file( fname )
	print( str(meta) )
	return 0

def enter_dir( indir, _opts, fs_subdirs, fs_files ):
	print( "entering dir "+indir+":"+indir2 )
	return 0

def leave_dir( indir, _opts, fs_subdirs, fs_files ):
	print( "leaving dir "+indir )
	return 0

# # # # # # # # # # # # # # # # # # # # #

# collect base info for the overall system
fsysrtn = get_fsys_list()
print( 'found fsys: '+str(fsysrtn) )

# TODO: check/verify what filesys we're on
flist = fsysrtn['list']
fsys = {}
for f in flist:
	if( basedir == f['root_dir'] ):
		fsys = f
		break
print( 'using fsys:'+str(fsys) )

print( 'crawling ['+basedir+']' )

exit()

for root,dirs,files in os.walk( basedir ):
	for name in files:
		print( 'file='+name )
		meta = get_file(name)
		if( meta['status'] == 'ok' ):
			print( '  file was found :: '+str(meta) )
		else:
			print( '  file was not found' )

