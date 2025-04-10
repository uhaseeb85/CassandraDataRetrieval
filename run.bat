@echo off
REM Cassandra to Kafka Exporter - Startup Script for Windows

REM Set Java options if needed
set JAVA_OPTS=-Xms512m -Xmx2g

REM Check if a custom config file is provided
set CONFIG_FILE=application.properties
if not "%~1"=="" (
    set CONFIG_FILE=%~1
    if not exist "%CONFIG_FILE%" (
        echo Error: Config file %CONFIG_FILE% not found!
        exit /b 1
    )
)

REM Copy the sample config file if no config exists
if not exist "%CONFIG_FILE%" (
    if exist "application.properties.sample" (
        echo No configuration file found. Creating one from sample...
        copy application.properties.sample "%CONFIG_FILE%" > nul
        echo Please edit %CONFIG_FILE% with your connection details before running the application.
        exit /b 1
    )
)

REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

REM Log startup
echo Starting Cassandra to Kafka Exporter...
echo Using configuration: %CONFIG_FILE%
echo Log files will be written to: %CD%\logs\

REM Start the application
java %JAVA_OPTS% -jar cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar "%CONFIG_FILE%"

REM Check exit status
if %ERRORLEVEL% neq 0 (
    echo Application exited with an error. Please check the logs.
    exit /b 1
) 