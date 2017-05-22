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

	public ArrayList<Sensor> getSensors(String appId);

	public boolean createMetric(Metric metric);

	public ArrayList<Metric> getMetrics(String sensorId);

	public boolean insertMeasurement(Metric data);

	public boolean insertMeasurements(ArrayList<Metric> metric);

	// Gets Measurement for a specific metric given two timestamps
	public ArrayList<Metric> getMeasurementsMetricFromTo(String metricId, long date1, long date2);

	// Creates connection to database
	public void connectToDb(String host, String port, String db, String user, String pass);

	public boolean closeConnection();

	// Reads properties ex(db.host=localhost db.user=username
	// db.password=password db.port=3306 db.name=DBNAME db.name=ADE)
	public void readProperties();

}
