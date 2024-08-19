copy ..\source\app\build\distributions\app.tar app.tar
docker build --build-arg APP_NAME=app -t frankenstein-app .
del app.tar