package databases.Cassandra;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class CassandraDbHandler implements IDbHandler {

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;

	Cluster cluster;
	Session session;

	private static GsonBuilder builder = new GsonBuilder();
	private static Gson gson = builder.create();

	private static final String CREATE_APP = "INSERT INTO Applications (" + " appId," + " appName,"
			+ " appDescription) VALUES (?, ?, ?)";
	private static PreparedStatement createApp;
	private static final String CREATE_SENSOR = "INSERT INTO Sensors (" + " appId," + " sensorId, " + " sensorName,"
			+ " sensorDescription) VALUES (?, ?, ?, ?);";
	private static PreparedStatement createSensor;
	private static final String CREATE_METRIC = "INSERT INTO Metrics (" + " appId," + " sensorId, " + "metricId, "
			+ " typeOfData," + " mUnit) VALUES (?, ?, ?, ?,?);";
	private static PreparedStatement createMetric;
	private static final String INSERT_MEASUREMENT = "INSERT INTO Measurements (" + " metricId," + " typeOfData,"
			+ " mUnit," + " value, " + "timestamp) VALUES (?,?,?, ?, ?);";
	private static PreparedStatement insertMeasurement;

	private static final String GET_APPS = "SELECT * FROM Applications;";
	private static PreparedStatement getApps;
	private static final String GET_APP = "SELECT * FROM Applications WHERE appId = ?;";
	private static PreparedStatement getApp;
	private static final String GET_SENSOR = "SELECT * FROM Sensors WHERE sensorId = ?;";
	private static PreparedStatement getSensor;
	private static final String GET_SENSORS = "SELECT * FROM Sensors;";
	private static PreparedStatement getSensors;
	private static final String GET_METRICS = "SELECT * FROM Metrics WHERE metricId = ?;";
	private static PreparedStatement getMetrics;
	private static final String GET_MEASUREMENTS = "SELECT * FROM Measurements WHERE (metricId = ?) AND (timestamp < ? AND timestamp >  ?));";
	private static PreparedStatement getMeasurements;

	public CassandraDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
		createApp = session.prepare(CREATE_APP);
		createSensor = session.prepare(CREATE_SENSOR);
		createMetric = session.prepare(CREATE_METRIC);
		insertMeasurement = session.prepare(INSERT_MEASUREMENT);
		getApps = session.prepare(GET_APPS);
		getApp = session.prepare(GET_APP);
		getSensor = session.prepare(GET_SENSOR);
		getSensors = session.prepare(GET_SENSORS);
		getMetrics = session.prepare(GET_METRICS);
		getMeasurements = session.prepare(GET_MEASUREMENTS);

	}

	public boolean createApp(Application app) {
		// TODO Auto-generated method stub
		BoundStatement bound = createApp.bind(app.getAppId(), app.getName(), app.getDesc());
		session.execute(bound);
		return true;
	}

	public Application getApp(String appId) {
		Application app = null;
		BoundStatement bound = getApp.bind(appId);
		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			app = new Application(row.getString(0), row.getString(1), row.getString(2));
			appToJson(app);
		}
		return app;
	}

	public ArrayList<Application> getApps() {
		// TODO Auto-generated method stub
		ArrayList<Application> apps = new ArrayList<Application>();
		BoundStatement bound = getApps.bind();
		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			Application app = new Application(row.getString(0), row.getString(1), row.getString(2));
			appToJson(app);
			apps.add(app);
		}
		return apps;
	}

	public boolean createSensor(Sensor sensor) {
		// TODO Auto-generated method stub
		BoundStatement bound = createSensor.bind(sensor.getAppId(), sensor.getSensorId(), sensor.getSensorName(),
				sensor.getSensorDesc());
		session.execute(bound);
		return true;
	}

	public Sensor getSensor(String sensorId) {
		// TODO Auto-generated method stub
		Sensor sensor = null;
		BoundStatement bound = getSensor.bind(sensorId);
		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			sensor = new Sensor(row.getString(0), row.getString(1), row.getString(2), row.getString(3));
			sensorToJson(sensor);
		}

		return sensor;
	}

	public ArrayList<Sensor> getSensors() {
		// TODO Auto-generated method stub
		ArrayList<Sensor> sensors = new ArrayList<Sensor>();
		BoundStatement bound = getSensors.bind();
		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			Sensor sensor = new Sensor(row.getString(0), row.getString(1), row.getString(2), row.getString(3));
			sensorToJson(sensor);
			sensors.add(sensor);
		}
		return sensors;
	}

	public boolean createMetric(Metric metric) {
		// TODO Auto-generated method stub
		BoundStatement bound = createMetric.bind(metric.getAppId(), metric.getSensorId(), metric.getMetricId(),
				metric.getTypeOfData(), metric.getmUnit());
		session.execute(bound);
		return true;
	}

	public ArrayList<Metric> getMetrics(String sensorId) {
		// TODO Auto-generated method stub
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		BoundStatement bound = getMetrics.bind(sensorId);
		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			Metric metric = new Metric(row.getString(0), row.getString(1), row.getString(2), row.getString(3),
					row.getString(4), row.getString(5), Long.parseLong(row.getString(5)));
			metricToJson(metric);
			metrics.add(metric);
		}
		return metrics;
	}

	public boolean insertMeasurement(Metric metric) {
		// TODO Auto-generated method stub
		BoundStatement bound = insertMeasurement.bind(metric.getMetricId(), metric.getTypeOfData(), metric.getmUnit(),
				metric.getValue(), metric.getTimestamp());
		session.execute(bound);
		return true;
	}

	public boolean insertMeasurements(ArrayList<Metric> metrics) {
		// TODO Auto-generated method stub
		BatchStatement batch = new BatchStatement();
		for (int i = 0; i < metrics.size(); i++) {
			batch.add(insertMeasurement.bind(metrics.get(i).getMetricId(), metrics.get(i).getTypeOfData(),
					metrics.get(i).getmUnit(), metrics.get(i).getValue(), metrics.get(i).getTimestamp()));
			System.out.println(batch.size());

		}
		session.execute(batch);
		return true;
	}

	public ArrayList<Metric> getMeasurementsMetricFromTo(Metric m, long date1, long date2) {
		// TODO Auto-generated method stub
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		BoundStatement bound;
		if (date1 > date2)
			bound = getMeasurements.bind(m, date1, date2);
		else
			bound = getMeasurements.bind(m, date2, date1);

		ResultSet rs = session.execute(bound);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			Metric metric = new Metric(row.getString(0), row.getString(1), row.getString(2), row.getString(3),
					row.getString(4), row.getString(5), Long.parseLong(row.getString(5)));
			metricToJson(metric);
			metrics.add(metric);
		}
		return metrics;
	}

	public boolean updateField(String table, String pkName, String id, String fieldName, String newValue) {
		// TODO Auto-generated method stub
		String update = "UPDATE " + table + " SET " + fieldName + " = '" + newValue + "' WHERE " + pkName + " = '" + id
				+ "';";
		session.execute(update);
		return true;
	}

	public boolean deleteRow(String table, String pkName, String id) {
		// TODO Auto-generated method stub
		String delete = "DELETE FROM " + table + " WHERE " + pkName + " = '" + id + "';";
		session.execute(delete);
		return true;
	}

	public boolean execStmnt(String query) {
		// TODO Auto-generated method stub
		session.execute(query);
		return true;

	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// TODO Auto-generated method stub
		// Creating Cluster.Builder object
		Cluster.Builder builder1 = Cluster.builder();
		// Adding contact point to the Cluster.Builder object
		Cluster.Builder builder2 = builder1.addContactPoint("52.26.188.76");
		// Building a cluster
		cluster = builder2.build();
		session = cluster.connect("ade");
	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
		if (cluster != null)
			cluster.close();
		if (session != null)
			session.close();
		System.out.println("Goodbye!");
		return true;
	}

	public boolean status() {
		// TODO Auto-generated method stub
		if (session != null && cluster != null) {
			return true;
		}
		return false;
	}

	public void readProperties() {
		try {
			@SuppressWarnings("resource")
			BufferedReader bf = new BufferedReader(new FileReader("src/main/resources/iot.properties"));
			String line = null;
			while ((line = bf.readLine()) != null) {
				String[] parts = line.split("=");
				if (parts[1] == null) {
					System.out.println("Missing properties!");
				} else {
					if (parts[0].compareTo("db.host") == 0) {
						host = parts[1];
					} else if (parts[0].compareTo("db.user") == 0) {
						user = parts[1];
					} else if (parts[0].compareTo("db.password") == 0) {
						password = parts[1];
					} else if (parts[0].compareTo("db.port") == 0) {
						port = parts[1];
					} else if (parts[0].compareTo("db.name") == 0) {
						dbname = parts[1];
					} else {
						System.out.println("Wrong properties!");
					}

				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String appToJson(Application app) {
		System.out.println(gson.toJson(app));
		return gson.toJson(app);
	}

	public String sensorToJson(Sensor sensor) {
		System.out.println(gson.toJson(sensor));
		return gson.toJson(sensor);

	}

	public String metricToJson(Metric metric) {
		System.out.println(gson.toJson(metric));
		return gson.toJson(metric);
	}

}
