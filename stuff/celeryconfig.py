#Backend Settings
CELERY_RESULT_BACKEND = "mongodb"
CELERY_MONGODB_BACKEND_SETTINGS = {
	"host": "localhost",
	"port": 27017,
	"database": "celery", 
	"taskmeta_collection": "worker_coll",
}
