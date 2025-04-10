@echo off
REM Script to package the Cassandra to Kafka Exporter for distribution on Windows

echo Packaging Cassandra to Kafka Exporter...

REM Create distribution directory
set DIST_DIR=dist
set PACKAGE_NAME=cassandra-to-kafka-exporter
set PACKAGE_DIR=%PACKAGE_NAME%-1.0-SNAPSHOT
set FULL_DIST_DIR=%DIST_DIR%\%PACKAGE_DIR%

REM Clean previous distribution if exists
if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
mkdir "%FULL_DIST_DIR%"

REM Build the project
echo Building project...
call mvn clean package

REM Copy files to distribution directory
echo Copying files to distribution directory...
copy target\cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar "%FULL_DIST_DIR%\"
copy application.properties.sample "%FULL_DIST_DIR%\"
copy run.sh "%FULL_DIST_DIR%\"
copy run.bat "%FULL_DIST_DIR%\"
copy README.md "%FULL_DIST_DIR%\"
copy INSTALL.md "%FULL_DIST_DIR%\"

REM Create ZIP archive (if 7-Zip is available)
echo Creating distribution package...
where 7z >nul 2>nul
if %ERRORLEVEL% equ 0 (
    cd %DIST_DIR%
    7z a -tzip "%PACKAGE_DIR%.zip" "%PACKAGE_DIR%"
    echo Distribution package created: %DIST_DIR%\%PACKAGE_DIR%.zip
) else (
    echo Warning: 7-Zip not found. Install 7-Zip to create ZIP archives automatically.
    echo Manual packaging required: ZIP the contents of %FULL_DIST_DIR%
)

echo Done! 