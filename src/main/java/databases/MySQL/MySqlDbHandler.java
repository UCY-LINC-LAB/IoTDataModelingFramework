package databases.MySQL;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class MySqlDbHandler implements IDbHandler {

	private static Connection conn = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;

	private static GsonBuilder builder = new GsonBuilder();
	private static Gson gson = builder.create();

	private static final String CREATE_APP = "INSERT INTO Applications (" + " appId," + " appName,"
			+ " appDescription) VALUES (?, ?, ?);";
	private static PreparedStatement createApp;
	private static final String CREATE_SENSOR = "INSERT INTO Sensors (" + " appId," + " sensorId, " + " sensorName,"
			+ " sensorDescription) VALUES (?, ?, ?, ?);";
	private static PreparedStatement createSensor;
	private static final String CREATE_METRIC = "INSERT INTO Metrics (" + " appId," + " sensorId, " + "metricId, "
			+ " typeOfData," + " mUnit) VALUES (?, ?, ?, ?,?);";
	private static PreparedStatement createMetric;
	private static final String INSERT_MEASUREMENT = "INSERT INTO Measurements (" + " metricId," + " value, "
			+ "timestamp) VALUES (?, ?, ?);";
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
	private static final String GET_MEASUREMENTS = "SELECT * FROM Measurements WHERE (metricId = ?) AND timestamp BETWEEN ? AND ?);";
	private static PreparedStatement getMeasurements;

	public MySqlDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
		try {
			createApp = conn.prepareStatement(CREATE_APP);
			createSensor = conn.prepareStatement(CREATE_SENSOR);
			createMetric = conn.prepareStatement(CREATE_METRIC);
			insertMeasurement = conn.prepareStatement(INSERT_MEASUREMENT);
			getApps = conn.prepareStatement(GET_APPS);
			getApp = conn.prepareStatement(GET_APP);
			getSensor = conn.prepareStatement(GET_SENSOR);
			getSensors = conn.prepareStatement(GET_SENSORS);
			getMetrics = conn.prepareStatement(GET_METRICS);
			getMeasurements = conn.prepareStatement(GET_MEASUREMENTS);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean createApp(Application app) {
		appToJson(app);
		try {
			createApp.setString(1, app.getAppId());
			createApp.setString(2, app.getName());
			createApp.setString(3, app.getDesc());
			createApp.executeUpdate();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			e.printStackTrace();
		}
		return false;
	}

	public Application getApp(String appId) {
		Application app = null;
		try {
			getApp.setString(1, appId);
			ResultSet rs = getApp.executeQuery();
			while (rs.next()) {
				appId = rs.getString("appId");
				String appName = rs.getString("appName");
				String appDescription = rs.getString("appDescription");
				app = new Application(appId, appName, appDescription);
				appToJson(app);
				return app;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return app;
	}

	public ArrayList<Application> getApps() {
		ArrayList<Application> apps = new ArrayList<Application>();
		try {
			ResultSet rs = getApps.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				String appName = rs.getString("appName");
				String appDescription = rs.getString("appDescription");
				Application app = new Application(appId, appName, appDescription);
				appToJson(app);
				apps.add(app);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return apps;
	}

	public boolean createSensor(Sensor sensor) {
		sensorToJson(sensor);
		try {
			createSensor.setString(1, sensor.getAppId());
			createSensor.setString(2, sensor.getSensorId());
			createSensor.setString(3, sensor.getSensorName());
			createSensor.setString(4, sensor.getSensorDesc());
			createSensor.executeUpdate();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public Sensor getSensor(String sensorId) {
		Sensor sensor = null;
		try {
			getSensor.setString(1, sensorId);
			ResultSet rs = getSensor.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				sensorId = rs.getString("sensorId");
				String sensorName = rs.getString("sensorName");
				String sensorDescription = rs.getString("sensorDescription");
				sensor = new Sensor(appId, sensorId, sensorName, sensorDescription);
				sensorToJson(sensor);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sensor;
	}

	public ArrayList<Sensor> getSensors() {
		ArrayList<Sensor> sensors = new ArrayList<Sensor>();
		try {
			ResultSet rs = getSensors.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				String sensorId = rs.getString("sensorId");
				String sensorName = rs.getString("sensorName");
				String sensorDescription = rs.getString("sensorDescription");
				Sensor sensor = new Sensor(appId, sensorId, sensorName, sensorDescription);
				sensors.add(sensor);
				sensorToJson(sensor);

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sensors;
	}

	public boolean createMetric(Metric metric) {
		metricToJson(metric);
		try {
			createMetric.setString(1, metric.getAppId());
			createMetric.setString(2, metric.getSensorId());
			createMetric.setString(3, metric.getMetricId());
			createMetric.setString(4, metric.getTypeOfData());
			createMetric.setString(5, metric.getmUnit());
			createMetric.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public ArrayList<Metric> getMetrics(String sensorId) {
		Metric metric = null;
		ArrayList<Metric> metrics = new ArrayList<Metric>();

		try {
			getMetrics.setString(1, sensorId);
			ResultSet rs = getMetrics.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				sensorId = rs.getString("sensorId");
				String metricId = rs.getString("metricId");
				String typeOfData = rs.getString("typeOfData");
				String mUnit = rs.getString("mUnit");
				metric = new Metric(appId, sensorId, metricId, typeOfData, mUnit, null, -1);
				metricToJson(metric);
				metrics.add(metric);

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return metrics;
	}

	public boolean insertMeasurement(Metric metric) {
		try {
			insertMeasurement.setString(1, metric.getMetricId());
			insertMeasurement.setString(2, metric.getValue());
			insertMeasurement.setLong(3, metric.getTimestamp());
			insertMeasurement.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean insertMeasurements(ArrayList<Metric> metrics) {
		PreparedStatement stmt;
		String batch = "INSERT INTO Measurements ( metricId, value, timestamp) VALUES (?, ?, ?)";
		String values = ", (?, ?, ?)";
		String end = ";";
		try {
			for (int i = 1; i < metrics.size(); i++) {
				batch = batch + values;
			}
			batch = batch + end;
			System.out.println(batch);
			stmt = conn.prepareStatement(batch);
			int position = 0;
			for (int i = 0; i < metrics.size(); i++) {
				position++;
				stmt.setString(position, metrics.get(i).getMetricId());
				position++;
				stmt.setString(position, metrics.get(i).getValue());
				position++;
				stmt.setLong(position, metrics.get(i).getTimestamp());
			}
			stmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public ArrayList<Metric> getMeasurementsMetricFromTo(String metricId, long date1, long date2) {
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		try {
			getMeasurements.setLong(1, date1);
			getMeasurements.setLong(2, date2);
			getMeasurements.setString(3, metricId);
			ResultSet rs = getMeasurements.executeQuery();
			while (rs.next()) {
				metricId = rs.getString("metricId");
				String value = rs.getString("value");
				long timestamp = rs.getLong("timestamp");
				Metric metric = new Metric(null, null, metricId, null, null, value, timestamp);
				metrics.add(metric);
				metricToJson(metric);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return metrics;
	}

	public boolean deleteRow(String table, String pkName, String id) {
		String delete = "DELETE FROM " + table + " WHERE " + pkName + " = '" + id + "';";
		try {
			stmt.executeUpdate(delete);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean updateField(String table, String pkName, String id, String fieldName, String newValue) {
		String update = "UPDATE " + table + " SET " + fieldName + " = '" + newValue + "' WHERE " + pkName + " = '" + id
				+ "';";
		try {
			stmt.executeUpdate(update);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean execStmnt(String query) {
		@SuppressWarnings("unused")
		ResultSet rs;
		try {
			rs = stmt.executeQuery(query);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// JDBC driver name and database URL
		final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
		// final String DB_URL = "jdbc:mysql://localhost/ADE";
		// String DB_URL = "jdbc:mysql://" + host + ":" + port + "/" + db +
		// "?useSSL=false";
		String DB_URL = "jdbc:mysql://" + host + ":" + port + "/" + db;

		if (pass.compareTo("\"\"") == 0) {
			pass = null;
		}

		System.out.println(DB_URL);
		// System.out.println(user);
		// System.out.println(pass);

		// STEP 2: Register JDBC driver
		try {
			Class.forName(JDBC_DRIVER);
			System.out.println("Connecting to a selected database...");
			DriverManager.setLoginTimeout(5);
			conn = DriverManager.getConnection(DB_URL, user, pass);
			System.out.println("Connected database successfully...");
			stmt = conn.createStatement();

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			System.out.println("Goodbye!");
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean status() {
		if (stmt != null && conn != null) {
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
