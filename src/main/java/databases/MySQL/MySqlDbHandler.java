package databases.MySQL;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import javax.sql.DataSource;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class MySqlDbHandler implements IDbHandler {

	private static ConnectionPool connPool;
	private static DataSource dataSource;

	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private final int timeout = 10; // connection time out.

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;

	private GsonBuilder builder = new GsonBuilder();
	private Gson gson = builder.create();

	private final String CREATE_APP = "INSERT INTO Applications (" + " appId," + " appName,"
			+ " appDescription) VALUES (?, ?, ?);";
	private PreparedStatement createApp;
	private final String CREATE_SENSOR = "INSERT INTO Sensors (" + " appId," + " sensorId, " + " sensorName,"
			+ " sensorDescription) VALUES (?, ?, ?, ?);";
	private PreparedStatement createSensor;
	private final String CREATE_METRIC = "INSERT INTO Metrics (" + " appId," + " sensorId, " + "metricId, "
			+ " typeOfData," + " mUnit) VALUES (?, ?, ?, ?,?);";
	private PreparedStatement createMetric;
	private final String INSERT_MEASUREMENT = "INSERT INTO Measurements (" + " metricId," + " value, "
			+ "timestamp) VALUES (?, ?, ?);";
	private PreparedStatement insertMeasurement;

	private final String GET_APPS = "SELECT * FROM Applications;";
	private PreparedStatement getApps;
	private final String GET_APP = "SELECT * FROM Applications WHERE appId = ?;";
	private PreparedStatement getApp;
	private final String GET_SENSOR = "SELECT * FROM Sensors WHERE sensorId = ?;";
	private PreparedStatement getSensor;
	private final String GET_SENSORS = "SELECT * FROM Sensors WHERE appId = ?;";
	private PreparedStatement getSensors;
	private final String GET_METRICS = "SELECT * FROM Metrics WHERE sensorId = ?;";
	private PreparedStatement getMetrics;
	private final String GET_MEASUREMENTS = "SELECT * FROM Measurements WHERE (metricId = ?) AND (timestamp BETWEEN ? AND ?);";
	private PreparedStatement getMeasurements;

	public MySqlDbHandler() {
		if (host == null) {
			readProperties();
		}

		if (conn == null) {
			connectToDb(host, port, dbname, user, password);
			// connectToDb("127.0.0.1", port, dbname, "root", "awsuser");
		}
		if (createApp == null) {
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

	}

	public boolean createApp(Application app) {
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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

	public ArrayList<Sensor> getSensors(String appId) {
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ArrayList<Sensor> sensors = new ArrayList<Sensor>();
		try {
			getSensors.setString(1, appId);
			ResultSet rs = getSensors.executeQuery();
			while (rs.next()) {
				appId = rs.getString("appId");
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PreparedStatement stmt;
		String batch = "INSERT INTO Measurements ( metricId, value, timestamp) VALUES (?, ?, ?)";
		String values = ", (?, ?, ?)";
		String end = ";";
		try {
			for (int i = 1; i < metrics.size(); i++) {
				batch = batch + values;
			}
			batch = batch + end;
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		try {
			getMeasurements.setString(1, metricId);
			getMeasurements.setLong(2, date1);
			getMeasurements.setLong(3, date2);
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
			return null;
		}
		return metrics;
	}

	public boolean deleteRow(String table, String pkName, String id) {
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		try {
			if (!conn.isValid(timeout)) {
				connectToDb(host, port, dbname, user, password);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
		String DB_URL = "jdbc:mysql://" + host + ":" + port + "/" + db
				+ "?verifyServerCertificate=false&useSSL=false&requireSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=1&serverTimezone=UTC";

		if (pass.compareTo("\"\"") == 0) {
			pass = null;
		}
		// if (connPool == null) {
		// connPool = new ConnectionPool();
		// try {
		// dataSource = connPool.setUp(JDBC_DRIVER, DB_URL, user, pass);
		// } catch (Exception e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }
		// // System.out.println("Connection pool created!");
		// }
		//
		// try {
		// conn = dataSource.getConnection();
		// stmt = conn.createStatement();
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		if (conn == null) {
			try {
				Class.forName(JDBC_DRIVER);
				// System.out.println("Connecting to a selected database...");
				DriverManager.setLoginTimeout(-1);
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
			// System.out.println("Goodbye!");
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

		Properties prop = new Properties();
		InputStream input = null;

		try {

			String filename = "iot.properties";
			input = MySqlDbHandler.class.getClassLoader().getResourceAsStream(filename);
			if (input == null) {
				System.out.println("Sorry, unable to find " + filename);
				return;
			}

			// load a properties file from class path, inside method
			prop.load(input);

			// get the property value and print it out
			// System.out.println(prop.getProperty("db.host"));
			// System.out.println(prop.getProperty("db.user"));
			// System.out.println(prop.getProperty("db.password"));
			// System.out.println(prop.getProperty("db.port"));
			// System.out.println(prop.getProperty("db.name"));

			host = prop.getProperty("db.host");
			user = prop.getProperty("db.user");
			password = prop.getProperty("db.password");
			port = prop.getProperty("db.port");
			dbname = prop.getProperty("db.name");

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String appToJson(Application app) {
		return gson.toJson(app);
	}

	public String sensorToJson(Sensor sensor) {
		return gson.toJson(sensor);

	}

	public String metricToJson(Metric metric) {
		return gson.toJson(metric);
	}

}
