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
| /Applications                                                                                   |GET      |Returns details about every application in the database        
| /Sensors?$filter=appId eq 'appId value'                                                         |GET      |Returns sensors that belongs to the specific application
| /Metrics?$filter=sensorId eq 'sensorId value'                                                   |GET      |Returns metrics tha belongs to the specific sensor
| /Measurements?$filter=metricId eq 'metricId value' and timestamp gt 'timestamp 1' and timestamp lt 'timestamp 2' |GET |Returns measurements with timestamp value between the two timestamps for the specific metric
| /Applications?data=Json                                                                          |POST     |Creates new application
| /Sensors?data=Json                                                                               |POST     |Creates new sensor
| /Metrics?data=Json                                                                               |POST     |Creates new metric
| /Measurements?data=Json                                                                          |POST     |Insert a new measurement


## Usage 2
1. Generate IoTDataModelingFramework-jar-with-dependencies.jar
2. Run with java -jar IoTDataModelingFramework-jar-with-dependencies.jar
3. Arguments:
      1. Number of sensors that sends measurements (>0)
      2. Number of measurements to send (>0)
      3. Time period to send measurement (>=0)
      4. Immediate start (0,1)
      5. Logs location (String)
      6. Name of the logs (String)







2. Generate IoTDataModelingFramework-jar-with-dependencies.jar

