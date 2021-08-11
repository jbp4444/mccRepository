"""
Simple Flask App

"""

import seaborn as sns
import matplotlib.pyplot as plt
from io import StringIO
import base64

from flask_app import app, models, webui, auth_required, AUTHLVL, subs

from flask import abort, g, request, json, jsonify, Response, render_template

# the app instance is created in __init__

@app.route( app.config['BASEPATH']+'charts' )
def chart_show_endpoints():
	rtn = [ {'endpoint':'/charts', 'verb':'GET', 'info':'show the list of available charts'},
		{'endpoint':'/charts/<num>', 'verb':'GET', 'info':'get a specific chart by ID-number'} ]
	return jsonify(rtn)

# quick test
# @app.route( app.config['BASEPATH']+'charts/<xx>' )
# def chart_basic( xx=None ):
# 	rtn = { 'info':'api test', 'status':'ok', 'input':str(xx) }
# 	return jsonify(rtn)

@app.route( app.config['BASEPATH']+'charts/abc' )
def chart_basic( xx=None ):
	img = StringIO()
	sns.set_style("dark") #E.G.

	y = [1,2,3,4,5]
	x = [0,2,1,3,4]

	print( '0000000' )
	plt.plot(x,y)
	print( '1111111' )
	plt.savefig( img, format='png' )
	print( '2222222' )
	plt.close()
	print( '3333333' )
	img.seek(0)

	print( 'AAAAAAA' )
	plot_url = base64.b64encode(img.getvalue())
	print( 'BBBBBBB' )

	return render_template('test.html', plot_url=plot_url)


import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

@app.route( app.config['BASEPATH']+'charts/def' )
def plotView():
	# Generate plot
	fig = Figure()
	axis = fig.add_subplot(1, 1, 1)
	axis.set_title("title")
	axis.set_xlabel("x-axis")
	axis.set_ylabel("y-axis")
	axis.grid()
	axis.plot(range(5), range(5), "ro-")

	# Convert plot to PNG image
	pngImage = io.BytesIO()
	FigureCanvas(fig).print_png(pngImage)

	# Encode PNG image to base64 string
	pngImageB64String = "data:image/png;base64,"
	pngImageB64String += base64.b64encode(pngImage.getvalue()).decode('utf8')

	return render_template( "test_chart.html", image=pngImageB64String )


@app.route( app.config['BASEPATH']+'charts/filesize' )
def chart_file_size():
	# Generate plot
	KB = 1024
	MB = 1024*KB
	GB = 1024*MB
	TB = 1024*GB
	PB = 1024*TB

	fsize_list = []
	count = 0
	for f in models.FileObj.objects:
		fsize_list.append( f.file_size )
		count = count + 1

	print( 'count = %d'%(count) )

	bins = [ 0, KB, 0.5*MB, MB, 0.5*GB, GB, 0.5*TB, TB, 0.5*PB, PB ]
	plt.hist( fsize_list, bins=20, histtype = 'bar', rwidth= 0.5 )
	plt.xlabel('File Size')
	plt.ylabel('No. of Files')
	plt.title('File-Size Histogram')

	# Convert plot to PNG image
	pngImage = io.BytesIO()
	# plt.print_png(pngImage)
	plt.savefig( pngImage, format='png' )

	# Encode PNG image to base64 string
	pngImageB64String = "data:image/png;base64,"
	pngImageB64String += base64.b64encode(pngImage.getvalue()).decode('utf8')

	return render_template( "test_chart.html", image=pngImageB64String )

