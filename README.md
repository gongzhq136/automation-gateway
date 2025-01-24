# Automation Gateway

Forked from [automation-gateway](https://github.com/vogler75/automation-gateway)

This gateway is used in Factory-AAS project as middleware. It is an OPCUA client that synchronizes with the OPCUA server in CIROS to obtain updated data values. Meanwhile, it is also an logger, logging the updated value into database, e.g. InfluxDB.

However, the original version only support InfluxDB V1 and it is InfluxDB V2 that used in the Factory-AAS project. So in this version, the support for connecting InfluxDB V2 is added.

What's more, the original version can not handle the data type, such as boolean, during the logging in the way that we needed. So modification is implemented to fulfill our own requirement.

It also has the feature to automatically replace the hostname when running in docker environment. This is essential because it's always tricky to connect an OPCUA client running in Docker to an OPCUA Server running in host.
