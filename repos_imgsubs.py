
import os
import logging

import mongoengine

from repos_worker import celapp, fsys_list, fsys_primary
from repos_model import *
from repos_common import *

#from PIL import Image, UnidentifiedImageError
import PIL

import repos_imgsubs_exif as exifsubs

# # # # # # # # # # # # # # # # # # # #
## # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #

# TODO: this is not used ... remove it?
@celapp.task
def tag_image_basic( logicalpath, params ):
	print( 'tagging image-data for '+logicalpath )

	fobj = FileObj.objects.get( logical_path=logicalpath )

	is_image = False
	imgdata = {}

	# TODO: gotta be a better way to do this
	ext = ''
	i = logicalpath.rfind( '.' )
	if( i > 0 ):
		ext = logicalpath[(i+1):]

	if( (ext=='jpg') or (ext=='jpeg') ):
		imgdata['type'] = 'jpeg'
		is_image = True
	elif( ext == 'png' ):
		imgdata['type'] = 'png'
		is_image = True
	elif( ext == 'gif' ):
		imgdata['type'] = 'gif'
		is_image = True
	elif( (ext=='tif') or (ext=='tiff') ):
		imgdata['type'] = 'tiff'
		is_image = True

	if( is_image ):
		real_filename = to_local_path( logicalpath, fsys_primary['server_name'] )

		img = Image.open( real_filename )

		imgdata['width']  = img.width
		imgdata['height'] = img.height
		imgdata['mode']   = img.mode

		fobj.tags['image_data'] = imgdata
		fobj.save()

	return 0

@celapp.task
def tag_image_data( logicalpath, params ):
	# look at a file and tag it with image-specific tags
	# : should only be called with known image-types (based on file-extension)
	rtn = 0

	logger = logging.getLogger( 'mcc_main' )
	server_name = params['server_name']

	logger.info( 'processing image-tags for '+logicalpath )

	try:
		fobj = FileObj.objects.get( logical_path=logicalpath )
		lname = fobj.logical_path
		(lpath,ext) = os.path.splitext( lname )
		ext = ext.lower()
		local_path = to_local_path( lname, server_name )

		# use PIL to get basic metadata
		im = PIL.Image.open( local_path )
		# : will throw exception if not an image file

		kvtags = {}
		kvtags['image'] = True
		kvtags['image_type'] = im.format
		kvtags['image_mode'] = im.mode
		kvtags['image_width'] = im.width
		kvtags['image_height'] = im.height

		try:
			exif_dict = exifsubs.generate_exif_dict( im )
			# DateTime, DateTimeOriginal, DateTimeDigitized
			# device: Make, Model, Software
			#kvtags['image_device'] = exif_dict['Make']['processed'] +','+ exif_dict['Model']['processed'] +','+ exif_dict['Software']['processed']
			kvtags['image_datetime'] = exif_dict['DateTime']['processed']
		except Exception as e1:
			logger.warning( 'Cannot parse EXIF data: '+lname+' : '+str(e1) )

		im.close()

		for k,v in kvtags.items():
			fobj.tags[k] = v
		fobj.save()

	except PIL.UnidentifiedImageError as e:
		# not really an error ... just not an image file
		pass
	except mongoengine.DoesNotExist as e:
		logger.error( 'FileObj does not exist: ' + lname + ' : ' + str(e) )
		rtn = -1
	except Exception as e:
		logger.error( 'Exception: ' + lname + ' : ' + str(e) )
		rtn = -2

	return rtn
