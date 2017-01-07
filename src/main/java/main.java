import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.MySQL.MySqlDbHandler;

public class main {

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		readProperties();
		Application app = new Application(null, "app1", "sdfsd");
		Sensor sensor = new Sensor(app.getAppId(), null, "sensor1", "desc");
		MySqlDbHandler db = new MySqlDbHandler();
		db.connectToDb(host, port, dbname, user, password);
		db.createApp(app);
		db.createSensor(sensor);
		Metric metric = new Metric(app.getAppId(), sensor.getSensorId(), null, "typeOfData", "C", null, 0);
		db.createMunit(metric);
		metric.setValue("150");
		metric.setTimestamp(1561564562);
		db.insertMetric(metric);

		app = db.getApp(app.getAppId());
		sensor = db.getSensor(sensor.getSensorId());
		metric = db.getMunit(metric.getSensorId(), metric.getmUnit());
		metric = db.getMetric(metric.getMetricId());

		System.out.println(app.getAppId() + " " + app.getName());
		System.out.println(sensor.getAppId() + " " + sensor.getSensorId());

	}

	public static void readProperties() {
		try {
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
