package databases.Cassandra;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class CassandraDbHandler implements IDbHandler {

	public boolean status() {
		// TODO Auto-generated method stub
		return false;
	}

	public void closeConnection() {
		// TODO Auto-generated method stub

	}

	public void insert(String insertStmt) {
		// TODO Auto-generated method stub

	}

	public void delete(String query) {
		// TODO Auto-generated method stub

	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// TODO Auto-generated method stub

	}

	public void createApp(Application app) {
		// TODO Auto-generated method stub
		
	}

	public void createSensor(Sensor sensor) {
		// TODO Auto-generated method stub
		
	}

	public void createMunit(Metric munit) {
		// TODO Auto-generated method stub
		
	}

	public void insertMetric(Metric data) {
		// TODO Auto-generated method stub
		
	}

	public void execStmnt(String stmt) {
		// TODO Auto-generated method stub
		
	}

	public Application getApp(String appId) {
		// TODO Auto-generated method stub
		return null;
	}

	public Sensor getSensor(String sensorId) {
		// TODO Auto-generated method stub
		return null;
	}

	public Metric getMunit(String sensorId, String mUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	public Metric getMetric(String metricId) {
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

}
