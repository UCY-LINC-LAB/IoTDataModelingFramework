package Interface;

import java.util.ArrayList;

import beans.Application;
import beans.Metric;
import beans.Sensor;

public interface IDbHandler {

	public boolean createApp(Application app);

	public Application getApp(String appId);

	public ArrayList<Application> getApps();

	public boolean createSensor(Sensor sensor);

	public Sensor getSensor(String sensorId);

	public ArrayList<Sensor> getSensors();

	public boolean createMetric(Metric metric);

	public ArrayList<Metric> getMetrics(String sensorId);

	public boolean insertMeasurement(Metric data);

	public boolean insertMeasurements(ArrayList<Metric> metric);

	public ArrayList<Metric> getMeasurementsMetricFromTo(Metric m, long date1, long date2);

	public boolean deleteRow(String table, String pkName, String id);

	public boolean updateField(String table, String pkName, String id, String fieldName, String newValue);

	public boolean execStmnt(String query);

	public void connectToDb(String host, String port, String db, String user, String pass);

	public boolean closeConnection();

	public boolean status();

	public void readProperties();

	public String appToJson(Application app);

	public String sensorToJson(Sensor sensor);

	public String metricToJson(Metric metric);
}
