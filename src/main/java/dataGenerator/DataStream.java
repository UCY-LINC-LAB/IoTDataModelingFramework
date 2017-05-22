package dataGenerator;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import HttpHandler.HttpClass;
import Logger.Logger;
import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.Cassandra.CassandraDbHandler;
import databases.CouchBase.CouchBaseDbHandler;
import databases.MongoDB.MongoDbHandler;
import databases.MySQL.MySqlDbHandler;

public class DataStream implements Runnable {

	private GsonBuilder builder = new GsonBuilder();
	private Gson gson = builder.create();
	private final String entities[] = { "Applications", "Sensors", "Metrics", "Measurements" };
	private Application app;
	private Sensor sensor;
	private Metric metric;
	private boolean getDelay = false;
	// private int getDelayTime = 15000;// Maximum time to wait for server
	// response. ms

	// timestamp start from 0 and increases each time we sent a measurement
	private int timestamp = 0;
	private int sLen = 5; // metrics.value minimum characters
	private int sMax = 300; // metrics.value maximum characters
	private int streamNum;
	private int periodTime;
	private int appNum;
	private int sensorNum;
	private int rtt; // sum of each database write
	private int rttDelay; // sum of each database write and get the
							// measurement

	// HttpClass httpHandler = new HttpClass();
	private MySqlDbHandler mysql;
	private CassandraDbHandler cass;
	private MongoDbHandler mongo;
	private static CouchBaseDbHandler couch;

	public DataStream(int appNum, int sensorNum, int streamNum, int periodTime) {

		this.streamNum = streamNum;
		this.periodTime = periodTime;
		this.appNum = appNum;
		this.sensorNum = sensorNum;
		rtt = 0;
		if (DataGenerator.logsLocation.compareTo("mysqlLogs") == 0) {
			mysql = new MySqlDbHandler();
		} else if (DataGenerator.logsLocation.compareTo("cassandraLogs") == 0) {
			cass = new CassandraDbHandler();
		} else if (DataGenerator.logsLocation.compareTo("mongoLogs") == 0) {
			mongo = new MongoDbHandler();
		} else if (DataGenerator.logsLocation.compareTo("couchbaseLogs") == 0) {
			couch = new CouchBaseDbHandler();
		}

	}

	@Override
	public void run() {
		createEntities();

		// System.out.println(Thread.currentThread().getName() + " Start");
		for (int i = 0; i < streamNum; i++) {
			long startTime = System.currentTimeMillis();
			
			
			sendMeasurement();
			//getDelay(startTime);
			//readMeasurement();

			if (periodTime > 0) {
				try {
					TimeUnit.SECONDS.sleep(periodTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		String toWrite = "Sum of sensor " + sensorNum + " Sum " + rtt + " Average " + (rtt / (streamNum + 3)) + " ms";
		Logger.writeInsertTime(toWrite);
		DataGenerator.rtt.add((rtt / (streamNum + 3)));

		toWrite = "Sum of sensor delay " + sensorNum + " Sum " + rttDelay + " Average " + (rttDelay / (streamNum + 3))
				+ " ms";
		Logger.writeDelay(toWrite);
		DataGenerator.rttDelay.add((rttDelay / (streamNum + 3)));

		// httpHandler.closeCon();
		System.out.println(Thread.currentThread().getName() + " End.");

	}

	private void sendMeasurement() {
		timestamp++;
		metric.setTimestamp(timestamp);
		if (sLen >= sMax) {
			sLen = 5;
		}
		String value = generateString(sLen);
		sLen++;
		metric.setValue(value);
		try {
			long startTime = System.currentTimeMillis();
			if (mysql != null) {
				mysql.insertMeasurement(metric);
			} else if (cass != null) {
				cass.insertMeasurement(metric);
			} else if (mongo != null) {
				mongo.insertMeasurement(metric);
			} else if (couch != null) {
				couch.insertMeasurement(metric);
			}

			long endTime = System.currentTimeMillis();
			// String toWrite = "Start " + startTime + " End " + endTime + "
			// Difference " + (endTime - startTime) + " ms";
			// Logger.writeInsertTime(toWrite);
			rtt += (endTime - startTime);
			// httpHandler.sendPost(entities[3], metricToJson(metric));
			DataGenerator.reqPerSec++;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void readMeasurement() {
		ArrayList<Metric> getMetric = null;
		long t = System.currentTimeMillis();
		// wait 15 to get response and
		// cancel
		long end = t + 60000;
		long startTime = 0;
		startTime = System.currentTimeMillis();

		while (getMetric == null) {
			try {
				if (mysql != null) {
					getMetric = mysql.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (cass != null) {
					getMetric = cass.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (mongo != null) {
					getMetric = mongo.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (couch != null) {
					getMetric = couch.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (System.currentTimeMillis() > end) {
				System.out.println("Can't retrieve measurement");
				break;
			}
		}

		if (getMetric != null) {
			long endTime = System.currentTimeMillis();
			rttDelay += (endTime - startTime);
		}
	}

	private void getDelay(long startTime2) {
		ArrayList<Metric> getMetric = null;
		long t = System.currentTimeMillis();
		// wait 15 to get response and
		// cancel
		long end = t + 60000;
		long startTime = startTime2;
		while (getMetric == null) {
			try {
				if (mysql != null) {
					getMetric = mysql.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (cass != null) {
					getMetric = cass.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (mongo != null) {
					getMetric = mongo.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				} else if (couch != null) {
					getMetric = couch.getMeasurementsMetricFromTo(metric.getMetricId(), metric.getTimestamp(),
							metric.getTimestamp());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (System.currentTimeMillis() > end) {
				System.out.println("Can't retrieve measurement");
				break;
			}
		}

		if (getMetric != null) {
			long endTime = System.currentTimeMillis();
			rttDelay += (endTime - startTime);
		}
	}

	private String generateString(int length) {
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = (char) (Math.random() * (122 - 97 + 1) + 97);
		}
		return new String(text);
	}

	/**
	 * Creates Application,Sensor,Metric
	 */
	private void createEntities() {
		try {
			// Create Application
			String appName = "app" + (appNum + 1);
			app = new Application(null, appName, "desc");
			// httpHandler.sendPost(entities[0], appToJson(app));

			long startTime = System.currentTimeMillis();
			if (mysql != null) {
				mysql.createApp(app);
			} else if (cass != null) {
				cass.createApp(app);
			} else if (mongo != null) {
				mongo.createApp(app);
			} else {
				couch.createApp(app);
			}

			long endTime = System.currentTimeMillis();
			// String toWrite = "Start " + startTime + " End " + endTime + "
			// Difference " + (endTime - startTime) + " ms";
			// Logger.writeInsertTime(toWrite);
			rtt += (endTime - startTime);
			DataGenerator.reqPerSec++;
			// Create Sensor
			String sensorName = appName + "_sensor" + (sensorNum + 1);
			sensor = new Sensor(app.getAppId(), null, sensorName, "desc");
			// httpHandler.sendPost(entities[1], sensorToJson(sensor));

			startTime = System.currentTimeMillis();

			if (mysql != null) {
				mysql.createSensor(sensor);
			} else if (cass != null) {
				cass.createSensor(sensor);
			} else if (mongo != null) {
				mongo.createSensor(sensor);
			} else {
				couch.createSensor(sensor);
			}

			endTime = System.currentTimeMillis();
			// toWrite = "Start " + startTime + " End " + endTime + " Difference
			// " + (endTime - startTime) + " ms";
			// Logger.writeInsertTime(toWrite);
			rtt += (endTime - startTime);

			DataGenerator.reqPerSec++;
			// Create Metric
			int r = 10;
			String dataType = generateString(r);
			metric = new Metric(app.getAppId(), sensor.getSensorId(), null, dataType, dataType, "", 0);
			// httpHandler.sendPost(entities[2], metricToJson(metric));

			startTime = System.currentTimeMillis();

			if (mysql != null) {
				mysql.createMetric(metric);
			} else if (cass != null) {
				cass.createMetric(metric);
			} else if (mongo != null) {
				mongo.createMetric(metric);
			} else {
				couch.createMetric(metric);
			}

			endTime = System.currentTimeMillis();
			// toWrite = "Start " + startTime + " End " + endTime + " Difference
			// " + (endTime - startTime) + " ms";
			// Logger.writeInsertTime(toWrite);
			rtt += (endTime - startTime);

			DataGenerator.reqPerSec++;
		} catch (Exception e) {
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
