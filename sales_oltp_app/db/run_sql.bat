@echo off
REM Run the Sales OLTP DDL as root on Zeenie. Output -> run.log
REM Do NOT hardcode the root password. Pre-set MYSQL_PWD in your shell, or this prompts for it.
if not defined MYSQL_PWD set /p "MYSQL_PWD=MySQL root password: "
set "MYSQL=C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"
set "D=C:\lakehouse\sales_sql"
"%MYSQL%" -u root < "%D%\00_create_db.sql" > "%D%\run.log" 2>&1
echo 00_create_db EXIT=%ERRORLEVEL% >> "%D%\run.log"
"%MYSQL%" -u root sales_oltp < "%D%\01_schema.sql" >> "%D%\run.log" 2>&1
echo 01_schema EXIT=%ERRORLEVEL% >> "%D%\run.log"
"%MYSQL%" -u root sales_oltp < "%D%\02_users.sql" >> "%D%\run.log" 2>&1
echo 02_users EXIT=%ERRORLEVEL% >> "%D%\run.log"
"%MYSQL%" -u root sales_oltp < "%D%\03_seed_reference.sql" >> "%D%\run.log" 2>&1
echo 03_seed EXIT=%ERRORLEVEL% >> "%D%\run.log"
"%MYSQL%" -u root -e "SHOW TABLES IN sales_oltp; SELECT COUNT(*) AS categories FROM sales_oltp.product_categories; SELECT COUNT(*) AS stores FROM sales_oltp.stores; SELECT user,host FROM mysql.user WHERE user LIKE 'sales%%';" >> "%D%\run.log" 2>&1
echo DONE >> "%D%\run.log"
