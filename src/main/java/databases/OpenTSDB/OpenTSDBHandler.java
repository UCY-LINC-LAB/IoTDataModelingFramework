package databases.OpenTSDB;

import java.util.ArrayList;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class OpenTSDBHandler implements IDbHandler {

	public boolean createApp(Application app) {
		// TODO Auto-generated method stub
		return false;
	}

	public Application getApp(String appId) {
		// TODO Auto-generated method stub
		return null;
	}

	public ArrayList<Application> getApps() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean createSensor(Sensor sensor) {
		// TODO Auto-generated method stub
		return false;
	}

	public Sensor getSensor(String sensorId) {
		// TODO Auto-generated method stub
		return null;
	}

	public ArrayList<Sensor> getSensors() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean createMetric(Metric metric) {
		// TODO Auto-generated method stub
		return false;
	}

	public ArrayList<Metric> getMetrics(String sensorId) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean insertMeasurement(Metric data) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean insertMeasurements(ArrayList<Metric> metric) {
		// TODO Auto-generated method stub
		return false;
	}

	public ArrayList<Metric> getMeasurementsMetricFromTo(Metric m, long date1, long date2) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean deleteRow(String table, String pkName, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean updateField(String table, String pkName, String id, String fieldName, String newValue) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean execStmnt(String query) {
		// TODO Auto-generated method stub
		return false;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// TODO Auto-generated method stub

	}

	public boolean closeConnection() {
		return false;
		// TODO Auto-generated method stub
	}

	public boolean status() {
		// TODO Auto-generated method stub
		return false;
	}

	public void readProperties() {
		// TODO Auto-generated method stub

	}

	public String appToJson(Application app) {
		// TODO Auto-generated method stub
		return null;
	}

	public String sensorToJson(Sensor sensor) {
		// TODO Auto-generated method stub
		return null;
	}

	public String metricToJson(Metric metric) {
		// TODO Auto-generated method stub
		return null;
	}

}
