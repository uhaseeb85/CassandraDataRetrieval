@echo off
setlocal enabledelayedexpansion

echo Running JUnit tests for Cassandra to Kafka Exporter
echo =================================================

rem Check if JAVA_HOME is set, or try to find Java
if "%JAVA_HOME%" == "" (
    where java >nul 2>nul
    if %ERRORLEVEL% == 0 (
        echo Using Java from PATH
    ) else (
        echo ERROR: JAVA_HOME environment variable is not set and Java is not in PATH.
        echo Please set JAVA_HOME to point to your Java installation directory.
        exit /b 1
    )
)

rem Check if Maven is available
where mvn >nul 2>nul
if %ERRORLEVEL% == 0 (
    echo Using Maven from PATH
    
    rem Run Maven tests
    echo Running Maven tests...
    call mvn test
) else (
    echo WARNING: Maven not found in PATH
    echo Attempting to use alternative test method...
    
    rem Try to use Maven Wrapper if available
    if exist "mvnw.cmd" (
        echo Found Maven Wrapper, using it to run tests...
        call mvnw.cmd test
    ) else (
        echo Maven Wrapper not found.
        echo Please install Maven or make sure it's in your PATH.
        echo You can also run 'package.bat' first to download dependencies.
        exit /b 1
    )
)

echo =================================================
echo Test execution complete.

exit /b 0 