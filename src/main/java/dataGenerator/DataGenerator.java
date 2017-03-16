package dataGenerator;

import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.MySQL.MySqlDbHandler;

public class DataGenerator {

	private final static String USER_AGENT = "Mozilla/5.0";

	private static MySqlDbHandler db = new MySqlDbHandler();
	// private static CassandraDbHandler db = new CassandraDbHandler();
	// private static MongoDbHandler db = new MongoDbHandler();

	private static String metric[] = { "Temperature", "Humidity", "UV index", "Wind", "Pressure", "Location", "Speed",
			"Comment" };
	private static String units[] = { "C", "%", "", "km/h", "hPa", "Coordinates", "km/h", "" };
	private static String types[] = { "One Dimensional", "One Dimensional", " One Dimensional", "MultiDimensional",
			"MultiDimensional", "Location", "MultiDimensional", "Text" };

	private final static int appNum = 2;
	private final static int sensorNum = 2;
	private final static int mUnitsNum = 3;
	private final static int time = 5000;
	private final static int streamNum = 2;
	private final static int stringlen = 300;
	private final static int numlen = 100;

	private static Application apps[] = new Application[appNum];
	private static Sensor sensors[][] = new Sensor[appNum][sensorNum];
	private static Metric metrics[][][] = new Metric[appNum][sensorNum][mUnitsNum];

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		createDb();
		System.out.println("Streaming data...");
		// streamData();

		// Application app = new Application(null, "sdfsd", "wrgr");
		// db.createApp(app);
		// Sensor sensor = new Sensor(app.getAppId(), null, "sensor1", "sdfsd");
		// db.createSensor(sensor);
		// long unixTime = System.currentTimeMillis() / 1000L;
		// Metric metric1 = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "10", "10001", "fdgfD", unixTime);
		// unixTime = System.currentTimeMillis() / 1000L;
		// Metric metric2 = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "10", "10001", "fdgfD", unixTime + 1);
		//
		// db.createMetric(metric1);
		// db.createMetric(metric2);
		// ArrayList<Metric> metrics = new ArrayList<Metric>();
		// metrics.add(metric1);
		// metrics.add(metric2);
		// db.insertMeasurements(metrics);
		// db.getApps();
		// db.getSensors();
		// db.getApp(app.getAppId());
		// db.getSensor(sensor.getSensorId());
		// db.getMeasurementsMetricFromTo(metric1.getMetricId(), 1486735488,
		// 1486735491);

	}

	/**
	 * Creates Application and sensors
	 */
	public static void createDb() {
		for (int i = 0; i < appNum; i++) {
			String appName = "app" + (i + 1);
			apps[i] = new Application(null, appName, "desc");
			// db.createApp(apps[i]);
			try {
				sendPost("Applications", db.appToJson(apps[i]));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int j = 0; j < sensorNum; j++) {
				String sensorName = appName + "_sensor" + (j + 1);
				sensors[i][j] = new Sensor(apps[i].getAppId(), null, sensorName, "desc");
				// db.createSensor(sensors[i][j]);
				try {
					sendPost("Sensors", db.sensorToJson(sensors[i][j]));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				metrics[i][j][0] = new Metric(apps[i].getAppId(), sensors[i][j].getSensorId(), null, "Text", "", null,
						0);
				// db.createMetric(metrics[i][j][k]);
				try {
					sendPost("Metrics", db.metricToJson(metrics[i][j][0]));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				metrics[i][j][1] = new Metric(apps[i].getAppId(), sensors[i][j].getSensorId(), null, "One Dimensional",
						"C", null, 0);
				// db.createMetric(metrics[i][j][k]);
				try {
					sendPost("Metrics", db.metricToJson(metrics[i][j][1]));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				metrics[i][j][2] = new Metric(apps[i].getAppId(), sensors[i][j].getSensorId(), null, "One Dimensional",
						"hPa", null, 0);
				// db.createMetric(metrics[i][j][k]);
				try {
					sendPost("Metrics", db.metricToJson(metrics[i][j][2]));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
								int randomtime = (int) (Math.random() * (time) + 1000);
								int timeSleep = randomtime;
								try {
									Thread.sleep(timeSleep);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								long unixTime = System.currentTimeMillis() / 1000L;
								metrics[appPos][sensorPos][metricPos].setTimestamp(unixTime);
								String value = generateString((int) (Math.random() * (20 - 1) + 1));
								value = String.valueOf(counter + 1);

								if (metrics[appPos][sensorPos][metricPos].getTypeOfData().compareTo("Text") != 0) {
									// int num = (int) (Math.random() * (300));
									value = String.valueOf((int) (Math.random() * (numlen)));

								} else {
									int num = (int) (Math.random() * (stringlen));
									value = String.valueOf(generateString(num));
								}

								metrics[appPos][sensorPos][metricPos].setValue(value);
								// db.insertMeasurement(metrics[appPos][sensorPos][metricPos]);
								try {
									sendPost("Measurements", db.metricToJson(metrics[appPos][sensorPos][metricPos]));
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								counter++;
							}
						}
					}.start();
				}
			}
		}
	}

	// HTTP POST request
	public static void sendPost(String object, String data) throws Exception {
		String postUrl = "http://localhost:8080/IoTDataModelingFramework/DemoService.svc/" + object;
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(postUrl);
		StringEntity postingString = new StringEntity(data);
		post.setEntity(postingString);
		post.setHeader("Content-type", "application/json");
		HttpResponse response = httpClient.execute(post);
	}

	public static String generateString(int length) {
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = (char) (Math.random() * (122 - 97 + 1) + 97);
		}
		return new String(text);
	}

}
