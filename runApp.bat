@echo off
python main.py
if errorlevel 1 (
    echo Proceso terminado con algun error.
) else (
    echo PROCESO EJECUTADO CON EXITO.

)
pause