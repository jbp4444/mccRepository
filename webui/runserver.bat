@echo off
setlocal
REM set PYTHONPATH="C:\Users\jbp1\Box\Home Folder jbp1\seq\repos\mcc"
set PYTHONPATH=C:\Users\jbp1\Box\Home Folder jbp1\seq\FileRepair\filerepair;C:\Users\jbp1\Box\Home Folder jbp1\seq\repos\mcc
python runserver.py %*
endlocal
