# mccRepository
repository-like tool based on MongoDB, Celery, and Click



reposadmin.py:
* aimed at basic manipulation of the data in the database
** not for "normal" operations
* file ops: create, read, update, delete, query, count, ckquery
* filesys ops: fsyscreate, fsysread, fsysupdate, fsysdelete, fsysquery
* dir ops: dircreate, dirread, dirupdate, dirdelete, dirquery
** directories are currently an odd mix of related-file-groups and directory/folders in file-system
** this is really to allow users to "claim" groups of files that they are responsible for
* user ops: usercreate, userread, userupdate, userdelete, userquery
* wipe: wipe out/delete ALL info in database (all file/fsys/dir/user data)
* logpath: show logical path for a given file

nightly.py:
* "normal" nightly/periodic operations
* scan - walk a file-system and ingest new files
* sync - push any new/missing files from primary to secondary
** submits copy_file tasks to back-end
* verify - check fixity info on files
** submits verify_existing_file tasks to back-end
* checkpolicy - checks files for compliance with "policies"
** file size >= 0; checksum is not blank; N copies of each file
* attachdirs - scan all files and attach them to known directories

reposops.py:
* "normal" operations
* dedupe - list duplicate checksums
* pending - query for files that have 'pending' info in their tags
* cleantags - remove file-objs from db that match a tag-query
* tagall - tag all files that match a given filename pattern
* filestats - calculate some basic stats on all files in a subdir

tagops.py:
* used for more complex tag-queries
* tagfullquery - walk through every item in database (could be slow)
* tagquery - Qfilter/Qcombination (faster) tag-query
* count - count objects matching the (simple) tag-query
* tagimages - simple scan for image-file metadata (all files in a subdir)
** submits tag_image_data tasks to the back-end
** png/jpeg/gif/tiff files; uses PIL to detect sizes and EXIF data
** stored into tag-list with keys 'image' (T/F), image_type/_mode/_width/_height/_device/_datetime
* walktag - walk a subdir and tag all files in it

repos_worker.py:
* main worker-bee code that processes background tasks
* connects to celery-db (in mongo) and waits for tasks
* includes repos_worksubs.py and repos_imagesubs.py (which includes repos_imagesubs_exif.py)

reposrepair.py:
* more of an example on how to write aux modules to work with the system
** works with repos_filerepair.py on the backend
* calcrepair - submits calc_repair tasks for all files in a subdir
** stores repair info into tag-list (key is 'filerepair')
* verifyrepair - submits verify_repair tasks for all files in a subdir
* wiperepair - submits wipe_repair tasks for all files in a subdir
** removes the 'filerepair' key from the tag-list

tapecheck.py:
* simulator for tape-based file-systems
* tapescan - process simulated data on a per-tape basis
** assumes a way to get all the files that live on a given tape
