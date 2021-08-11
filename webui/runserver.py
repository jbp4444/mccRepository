#!/usr/bin/env python
"""
This script runs the flask_app application using a development server.
"""

import os
import webbrowser
import threading

from flask_app import app


if __name__ == '__main__':
	# launch the browser in 3 sec
	if( 'WERKZEUG_RUN_MAIN' not in os.environ ):
		threading.Timer( 3.33, lambda: webbrowser.open('http://localhost:5000/admin/',new=1,autoraise=True) ).start()

	# run the Flask app (and server)
	app.run( 'localhost', 5000 )
