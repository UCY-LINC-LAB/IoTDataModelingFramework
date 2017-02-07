import java.util.ArrayList;

import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.Cassandra.CassandraDbHandler;
import databases.MySQL.MySqlDbHandler;

public class main {

	// private static MySqlDbHandler db = new MySqlDbHandler();
	private static CassandraDbHandler db = new CassandraDbHandler();

	private final static int appNum = 5;
	private final static int sensorNum = 5;
	private final static int mUnitsNum = 2;
	private static int streamNum = 3;

	private static Application apps[] = new Application[appNum];
	private static Sensor sensors[][] = new Sensor[appNum][sensorNum];
	private static Metric metrics[][][] = new Metric[appNum][sensorNum][mUnitsNum];

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// createDb();
		// System.out.println("Streaming data...");
		// streamData();

		Application app = new Application(null, "sdfsd", "wrgr");
		db.createApp(app);
		Sensor sensor = new Sensor(app.getAppId(), null, "sensor1", "sdfsd");
		db.createSensor(sensor);
		long unixTime = System.currentTimeMillis() / 1000L;
		Metric metric1 = new Metric(app.getAppId(), sensor.getSensorId(), null, "10", "10001", "fdgfD", unixTime);
		unixTime = System.currentTimeMillis() / 1000L;
		Metric metric2 = new Metric(app.getAppId(), sensor.getSensorId(), null, "10", "10001", "fdgfD", unixTime+1);

		db.createMetric(metric1);
		db.createMetric(metric2);
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		metrics.add(metric1);
		metrics.add(metric2);
		db.insertMeasurements(metrics);

		// db.getApps();
		// db.getSensors();
		// db.getApp(app.getAppId());
		// db.getSensor(sensor.getSensorId());

	}

	/**
	 * Creates Application and sensors
	 */
	public static void createDb() {
		for (int i = 0; i < appNum; i++) {
			String appName = "app" + (i + 1);
			apps[i] = new Application(null, appName, "desc");
			db.createApp(apps[i]);
			for (int j = 0; j < sensorNum; j++) {
				String sensorName = appName + "_sensor" + (j + 1);
				sensors[i][j] = new Sensor(apps[i].getAppId(), null, sensorName, "desc");
				db.createSensor(sensors[i][j]);
				for (int k = 0; k < mUnitsNum; k++) {
					String mUnit = generateString((int) (Math.random() * (20 - 1) + 1));
					// System.out.println(appName + " " + sensorName + " " +
					// mUnit);
					metrics[i][j][k] = new Metric(apps[i].getAppId(), sensors[i][j].getSensorId(), null, "Text", mUnit,
							null, 0);
					db.createMetric(metrics[i][j][k]);
				}
			}
		}
	}

	public static void streamData() {
		for (int i = 0; i < appNum; i++) {
			for (int j = 0; j < sensorNum; j++) {
				for (int k = 0; k < mUnitsNum; k++) {
					final int appPos = i;
					final int sensorPos = j;
					final int metricPos = k;
					new Thread() {
						public void run() {
							int counter = 0;
							while (counter < streamNum || streamNum == 0) {
								int time = 1000;
								try {
									Thread.sleep(time);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								long unixTime = System.currentTimeMillis() / 1000L;
								metrics[appPos][sensorPos][metricPos].setTimestamp(unixTime);
								String value = generateString((int) (Math.random() * (20 - 1) + 1));
								metrics[appPos][sensorPos][metricPos].setValue(value);
								db.insertMeasurement(metrics[appPos][sensorPos][metricPos]);
								counter++;
							}

						}
					}.start();
				}
			}
		}
	}

	public static String generateString(int length) {
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = (char) (Math.random() * (122 - 97 + 1) + 97);
		}
		return new String(text);
	}

}
