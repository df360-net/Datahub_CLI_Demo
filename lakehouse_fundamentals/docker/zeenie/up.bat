@echo off
REM Bring the Wave 0 lakehouse up on Zeenie. Deployed to C:\lakehouse on the Dell.
REM First run (image pulls) must happen in the interactive console session - either run
REM this directly at the Dell's terminal, or via a one-shot Scheduled Task (see README),
REM because Docker Desktop's credential helper fails over plain SSH (session 0).
cd /d C:\lakehouse
docker compose up -d > C:\lakehouse\up.log 2>&1
echo EXITCODE=%ERRORLEVEL% >> C:\lakehouse\up.log
echo DONE >> C:\lakehouse\up.log
