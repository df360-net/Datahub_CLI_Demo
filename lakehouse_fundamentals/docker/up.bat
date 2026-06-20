@echo off
cd /d C:\lakehouse
echo [START] > C:\lakehouse\up.log
docker compose up -d >> C:\lakehouse\up.log 2>&1
echo EXITCODE=%ERRORLEVEL%>> C:\lakehouse\up.log
echo [DONE] >> C:\lakehouse\up.log
