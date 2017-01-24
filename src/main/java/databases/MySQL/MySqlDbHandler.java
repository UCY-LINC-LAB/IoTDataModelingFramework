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

	private static final String CREATE_APP = "INSERT INTO Applications (" + " appId," + " appName,"
			+ " appDescription) VALUES (?, ?, ?)";
	private static final String CREATE_SENSOR = "INSERT INTO Sensors (" + " appId," + " sensorId, " + " sensorName,"
			+ " sensorDescription) VALUES (?, ?, ?, ?)";
	private static final String CREATE_M_UNIT = "INSERT INTO MeasurementUnits (" + " appId," + " sensorId, "
			+ " typeOfData," + " mUnit) VALUES (?, ?, ?, ?)";
	private static final String INSERT_METRIC = "INSERT INTO Metrics (" + " metricId," + " value, "
			+ "timestamp) VALUES (?, ?, ?)";
	private static final String GET_APP = "SELECT * FROM Applications WHERE appId = ?";
	private static final String GET_SENSOR = "SELECT * FROM Sensors WHERE sensorId = ?";
	private static final String GET_M_UNIT = "SELECT * FROM MeasurementUnits WHERE sensorId = ? AND mUnit = ?";
	private static final String GET_METRIC = "SELECT * FROM Metrics WHERE metricId = ?";

	public MySqlDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
	}

	public void createApp(Application app) {
		try {
			PreparedStatement stmt = conn.prepareStatement(CREATE_APP);
			stmt.setString(1, app.getAppId());
			stmt.setString(2, app.getName());
			stmt.setString(3, app.getDesc());
			stmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			e.printStackTrace();
		}

	}

	public Application getApp(String appId) {
		Application app = null;
		try {
			PreparedStatement stmt = conn.prepareStatement(GET_APP);
			stmt.setString(1, appId);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				appId = rs.getString("appId");
				String appName = rs.getString("appName");
				String appDescription = rs.getString("appDescription");
				app = new Application(appId, appName, appDescription);
				return app;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return app;
	}

	public void createSensor(Sensor sensor) {
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(CREATE_SENSOR);
			stmt.setString(1, sensor.getAppId());
			stmt.setString(2, sensor.getSensorId());
			stmt.setString(3, sensor.getSensorName());
			stmt.setString(4, sensor.getSensorDesc());
			stmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Sensor getSensor(String sensorId) {
		Sensor sensor = null;
		try {
			PreparedStatement stmt = conn.prepareStatement(GET_SENSOR);
			stmt.setString(1, sensorId);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				sensorId = rs.getString("sensorId");
				String sensorName = rs.getString("sensorName");
				String sensorDescription = rs.getString("sensorDescription");
				sensor = new Sensor(appId, sensorId, sensorName, sensorDescription);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sensor;
	}

	public void createMunit(Metric munit) {
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(CREATE_M_UNIT);
			stmt.setString(1, munit.getAppId());
			stmt.setString(2, munit.getSensorId());
			stmt.setString(3, munit.getTypeOfData());
			stmt.setString(4, munit.getmUnit());
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Metric getMunit(String sensorId, String mUnit) {
		Metric metric = null;
		try {
			PreparedStatement stmt = conn.prepareStatement(GET_M_UNIT);
			stmt.setString(1, sensorId);
			stmt.setString(2, mUnit);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String appId = rs.getString("appId");
				sensorId = rs.getString("sensorId");
				String typeOfData = rs.getString("typeOfData");
				mUnit = rs.getString("mUnit");
				metric = new Metric(appId, sensorId, null, typeOfData, mUnit, null, -1);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return metric;
	}

	public void insertMetric(Metric data) {
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(INSERT_METRIC);
			stmt.setString(1, data.getMetricId());
			stmt.setString(2, data.getValue());
			stmt.setLong(3, data.getTimestamp());
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public Metric getMetric(String metricId) {
		Metric metric = null;
		try {
			PreparedStatement stmt = conn.prepareStatement(GET_METRIC);
			stmt.setString(1, metricId);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				metricId = rs.getString("metricId");
				String value = rs.getString("value");
				long timestamp = rs.getLong("timestamp");
				metric = new Metric(null, null, metricId, null, null, value, timestamp);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return metric;
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

	public void execStmnt(String stmt) {
		ResultSet rs;
		try {
			rs = this.stmt.executeQuery(stmt);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// JDBC driver name and database URL
		final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
		// final String DB_URL = "jdbc:mysql://localhost/ADE";
		String DB_URL = "jdbc:mysql://" + host + ":" + port + "/" + db;

		if (pass.compareTo("\"\"") == 0) {
			pass = null;
		}

		System.out.println(DB_URL);
		// STEP 2: Register JDBC driver
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Connecting to a selected database...");
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

	public void closeConnection() {
		// TODO Auto-generated method stub
		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Goodbye!");
	}

	public boolean status() {
		if (stmt != null && conn != null) {
			return true;
		}
		return false;
	}

	public static void readProperties() {
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

}
