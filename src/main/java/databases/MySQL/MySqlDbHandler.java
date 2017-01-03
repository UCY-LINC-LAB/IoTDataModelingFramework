package databases.MySQL;

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

	private static final String CREATE_APP = "INSERT INTO Applications (" + " appId," + " appName,"
			+ " appDescription) VALUES (?, ?, ?)";
	private static final String CREATE_SENSOR = "INSERT INTO Sensors (" + " appId," + " sensorId, " + " sensorName,"
			+ " sensorDescription) VALUES (?, ?, ?, ?)";
	private static final String CREATE_M_UNIT = "INSERT INTO MeasurementUnits (" + " appId," + " sensorId, "
			+ " typeOfData," + " mUnit) VALUES (?, ?, ?, ?)";
	private static final String INSERT_METRIC = "INSERT INTO Measurements (" + " metricId," + " value, "
			+ "timestamp) VALUES (?, ?, ?)";

	public MySqlDbHandler() {

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
			e.printStackTrace();
		}

	}

	public Application getApp(String appId) {
		Application app = null;
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

	public void insertMetric(Metric data) {
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(INSERT_METRIC);
			stmt.setString(1, data.getSensorId() + "$" + data.getmUnit());
			stmt.setString(2, data.getValue());
			stmt.setLong(3, data.getTimestamp());
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

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

}
