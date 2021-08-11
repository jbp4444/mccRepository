
import os
import sys

import mutagen

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

def get_music_data( local_path ):
	# look at a file and tag it with image-specific tags
	# : should only be called with known image-types (based on file-extension)
	rtn = 0

	try:
		# use PIL to get basic metadata
		mm = mutagen.File( local_path )
		print( str(mm) )

		if( mm is not None ):
			for (k,v) in mm.info.items():
				print( 'k=[%s]  v=[%s]'%(k,v) )

	except mutagen.MutagenError as e:
		print( 'Mutagen error: ' + str(e) )
		rtn = -1
	except Exception as e:
		print( 'Exception: ' + str(e) )
		rtn = -3

	return rtn

if __name__ == '__main__':
	get_music_data( sys.argv[1] )
