package databases.Hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class HbaseDbHandler implements IDbHandler {

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;

	private static GsonBuilder builder = new GsonBuilder();
	private static Gson gson = builder.create();

	public HbaseDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
	}

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

	public ArrayList<Sensor> getSensors(String appId) {
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

	public ArrayList<Metric> getMeasurementsMetricFromTo(String metricId, long date1, long date2) {
		// TODO Auto-generated method stub
		return null;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		// TODO Auto-generated method stub

		System.out.println("Creating Htable starts");
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "54.69.202.140");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Not connected!!");
			e.printStackTrace();
		}

	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
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
		return gson.toJson(app);
	}

	public String sensorToJson(Sensor sensor) {
		return gson.toJson(sensor);

	}

	public String metricToJson(Metric metric) {
		return gson.toJson(metric);
	}

}
