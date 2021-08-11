@echo off
setlocal
REM set PYTHONPATH=C:\Users\jbp1\Box\Home Folder jbp1\seq\FileRepair\filerepair
set PYTHONPATH="C:\Users\jbp1\Box\Home Folder jbp1\seq\repos\mcc"
celery -A repos_worker worker --pool=solo -l INFO -Q blue
endlocal
