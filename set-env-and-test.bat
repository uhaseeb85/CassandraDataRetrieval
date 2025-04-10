@echo off
setlocal enabledelayedexpansion

echo Setting up environment for testing...

rem Try to find Java installation directly
if exist "C:\Program Files\Java" (
    echo Found Java directory at C:\Program Files\Java
    for /d %%i in ("C:\Program Files\Java\jdk*") do (
        echo Setting JAVA_HOME to: %%i
        set "JAVA_HOME=%%i"
        goto :found_java
    )
    for /d %%i in ("C:\Program Files\Java\jre*") do (
        echo Setting JAVA_HOME to: %%i
        set "JAVA_HOME=%%i"
        goto :found_java
    )
)

if exist "C:\Program Files (x86)\Java" (
    echo Found Java directory at C:\Program Files (x86)\Java
    for /d %%i in ("C:\Program Files (x86)\Java\jdk*") do (
        echo Setting JAVA_HOME to: %%i
        set "JAVA_HOME=%%i"
        goto :found_java
    )
    for /d %%i in ("C:\Program Files (x86)\Java\jre*") do (
        echo Setting JAVA_HOME to: %%i
        set "JAVA_HOME=%%i"
        goto :found_java
    )
)

rem If we can't find it, prompt the user
echo Could not find Java installation automatically.
echo Please enter the path to your Java installation (e.g., C:\Program Files\Java\jdk1.8.0_XXX):
set /p JAVA_HOME=

:found_java
echo Using JAVA_HOME: %JAVA_HOME%

echo.
echo Running Maven dependency:copy-dependencies to gather dependencies...
call .\mvnw.cmd dependency:copy-dependencies -DoutputDirectory=target/dependency

echo.
echo Running Maven test...
call .\mvnw.cmd test

echo.
echo Test execution completed. 