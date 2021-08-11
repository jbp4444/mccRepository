
import os
import logging

import mongoengine

from repos_worker import celapp, fsys_list, fsys_primary
from repos_model import *
from repos_common import *

import mutagen

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

@celapp.task
def tag_music_data( logicalpath, params ):
	# look at a file and tag it with image-specific tags
	# : should only be called with known image-types (based on file-extension)
	rtn = 0

	logger = logging.getLogger( 'mcc_main' )
	server_name = params['server_name']

	logger.info( 'processing music-tags for '+logicalpath )

	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		lname = fobj.logical_path
		(lpath,ext) = os.path.splitext( lname )
		ext = ext.lower()
		local_path = to_local_path( lname, server_name )

		# use PIL to get basic metadata
		mm = mutagen.File( local_path )

		kvtags = {}
		if( mm is not None ):
			kvtags['music'] = True
			kvtags['music_length'] = mm.info.length
			kvtags['music_bitrate'] = mm.info.bitrate
			# if( 'TAL' in mm.tags ):
			# 	# assume we want the first text-block for album name
			# 	kvtags['music_album'] = mm.tags['TAL'].text[0]
			# elif( 'TALB' in mm.tags ):
			# 	# assume we want the first text-block for album name
			# 	kvtags['music_album'] = mm.tags['TALB'].text[0]
			if( 'TCO' in mm.tags ):
				# assume we want the first text-block for album name
				kvtags['music_genre'] = mm.tags['TCO'].text[0]
			elif( 'TCON' in mm.tags ):
				# assume we want the first text-block for album name
				kvtags['music_genre'] = mm.tags['TCON'].text[0]

		for k,v in kvtags.items():
			fobj.tags[k] = v
		fobj.save()

	except mutagen.MutagenError as e:
		logger.error( 'Mutagen error: ' + lname + ' : ' + str(e) )
		rtn = -1
	except mongoengine.DoesNotExist as e:
		logger.error( 'FileObj does not exist: ' + lname + ' : ' + str(e) )
		rtn = -2
	except Exception as e:
		logger.error( 'Exception: ' + lname + ' : ' + str(e) )
		rtn = -3

	return rtn
