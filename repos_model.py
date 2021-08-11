"""
Simple Flask App

"""

import mongoengine as me


class UserObj(me.Document):
	username    = me.StringField( unique=True )
	password    = me.StringField( required=True )
	permissions = me.IntField()

	def __str__( self ):
		return 'User( username=%s, password=%s, permissions=%d )'%(self.username,self.password,self.permissions)

# # # # # # # # # # #
## # # # # # # # # #
# # # # # # # # # # #

# https://marciazeng.slis.kent.edu/metadatabasics/types.htm
# : admin_meta = acquisition info, rights/legal, location (we capture that in filesys)
# : descr_meta = catalog records, curation info, links to other docs
# : preserv_meta = physical conditions, actions taken, doc of changes
# : tech_meta = hw/sw info, digitization info, formats, compression
# : use_meta = usage/access info, circ records

class FilesysObj(me.DynamicDocument):
	server_name = me.StringField(unique=True)
	root_dir    = me.StringField()
	is_tape     = me.BooleanField()
	is_primary  = me.BooleanField()
	is_readonly = me.BooleanField()

	# simple tags and key-value tags
	tags          = me.DictField()

	# preserv_meta  = me.DictField()
	# tech_meta     = me.DictField()
	# admin_meta    = me.DictField()
	# descr_meta    = me.DictField()
	# rights_meta   = me.DictField()
	# usage_meta      = me.DictField()

	def __str__(self):
		tape = 'N'
		if( self.is_tape ):
			tape = 'Y'
		primary = 'N'
		if( self.is_primary ):
			primary = 'Y'
		readonly = 'N'
		if( self.is_readonly ):
			readonly = 'Y'
		return 'FilesysObj( server_name=%s, root_dir="%s", is_tape=%s, is_primary=%s, is_readonly=%s, tags=%s )'%(self.server_name,self.root_dir,tape,primary,readonly,str(self.tags))
	def __unicode__(self):
		return self.server_name

# abstract objects for directories
# : really this is more of a notification-group identifier
# : only one dir-obj per dir/group even if there are multiple file-systems
class DirObj(me.Document):
	logical_path   = me.StringField(unique=True)
	sponsor        = me.ReferenceField(UserObj)
	# TODO: could we store work-group-numbering here to, e.g., spread out cksum workload
	# worker_num     = me.IntField()

	# simple tags and key-value tags
	tags          = me.DictField()

	def __str__( self ):
		# return 'DirObj( logical_path=%s, sponsor=%s, worker_num=%d, tags=%s )'%(self.logical_path,str(self.sponsor),self.worker_num,str(self.tags))
		return 'DirObj( logical_path=%s, sponsor=%s, tags=%s )'%(self.logical_path,str(self.sponsor),str(self.tags))

# 	def __unicode__(self):
# 		return self.pathname

# class FileCopyObj(me.EmbeddedDocument):
# 	# TODO: this should be unique for each FileObj .. how to do that?
# 	copy_num    = me.IntField()
# 	filesys_obj = me.ReferenceField(FilesysObj)
# 	last_check  = me.DateTimeField()

class FileObj(me.DynamicDocument):
	logical_path  = me.StringField(unique=True)
	directory     = me.ReferenceField(DirObj)    # this is really a notification group
	file_size     = me.IntField()
	checksum      = me.DictField()
	# ALT: checksum      = me.StringField()
	# copies        = me.ListField(me.EmbeddedDocumentField(FileCopyObj))
	# alt: copies      = me.EmbeddedDocumentListField(FileCopyObj)
	copies        = me.DictField()

	# simple tags and key-value tags
	tags          = me.DictField()

	# preserv_meta  = me.DictField()
	# tech_meta     = me.DictField()
	# admin_meta    = me.DictField()
	# descr_meta    = me.DictField()
	# rights_meta   = me.DictField()
	# usage_meta      = me.DictField()

	def __str__(self):
		return 'FileObj( logical_path="%s", dir=%s, file_size=%d, checksum=%s, copies=%s, tags=%s )' % (self.logical_path,str(self.directory),self.file_size,self.checksum,str(self.copies),str(self.tags))

