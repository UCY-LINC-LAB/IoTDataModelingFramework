# IoTDataModelingFramework
----

## Usage 1
1. Generate IoTDataModelingFramework.war
2. Deploy IoTDataModelingFramework.war file in /opt/tomcat/webapps (tomcat version 8)
3. Execute $ sudo service tomcat restart
4. The webapp is accessible from http://localhost:8080/IoTDataModelingFramework/DemoService.svc/

----
## API

| Path                                                                                           	| Method 	| Response                                                        	|
|------------------------------------------------------------------------------------------------	|--------	|-----------------------------------------------------------------	|
| /$metadata                                                                                      |GET      |Returns metadata for all entities (application, sensor,metric, measurement)
| /$Applications                                                                                  |GET      |Returns details about every application in the database


2. Generate IoTDataModelingFramework-jar-with-dependencies.jar

