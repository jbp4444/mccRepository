
import sys
import hashlib

def calc_cksum( infile, blksz=4096, algo='md5' ):
	ckobj = hashlib.new( algo )
	with open( infile,'rb') as f:
		for chunk in iter(lambda: f.read(blksz), b""):
			ckobj.update(chunk)
	return ckobj.hexdigest()

if __name__ == '__main__':
	cksm = calc_cksum( sys.argv[1] )
	print( '%s :: %s'%(sys.argv[1],cksm) )
