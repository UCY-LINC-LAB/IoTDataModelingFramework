package Interface;

import beans.Application;
import beans.Metric;
import beans.Sensor;

public interface IDbHandler {

	public void createApp(Application app);

	public Application getApp(String appId);

	public void createSensor(Sensor sensor);

	public Sensor getSensor(String sensorId);

	public void createMunit(Metric munit);

	public Metric getMunit(String sensorId, String mUnit);

	public void insertMetric(Metric data);

	public Metric getMetric(String metricId);

	public boolean deleteRow(String table, String pkName, String id);

	public boolean updateField(String table, String pkName, String id, String fieldName, String newValue);

	public void execStmnt(String stmt);

	public void connectToDb(String host, String port, String db, String user, String pass);

	public void closeConnection();

	public boolean status();

}
