package databases.CouchBase;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import com.couchbase.client.deps.io.netty.channel.rxtx.RxtxChannelConfig.Databits;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.MySQL.MySqlDbHandler;

public class CouchBaseDbHandler implements IDbHandler {

	private String host;
	private String dbname;
	private String port;
	private String user;
	private String password;
	private String appsBucketName = "Applications";
	private String sensorsBucketName = "Sensors";
	private String metricsBucketName = "Metrics";
	private String measurementsBucketName = "Measurements";

	private GsonBuilder builder = new GsonBuilder();
	private Gson gson = builder.create();

	private static Cluster cluster;
	private Bucket appsBucket;
	private Bucket sensorsBucket;
	private Bucket metricsBucket;
	private Bucket measurementsBucket;

	public CouchBaseDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
	}

	public boolean createApp(Application app) {
		try {
			JsonObject content = JsonObject.empty().put("appId", app.getAppId()).put("appName", app.getName())
					.put("appDesc", app.getDesc());
			JsonDocument doc = JsonDocument.create(app.getAppId(), content);
			JsonDocument response = appsBucket.insert(doc);
			return true;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return false;
		}
	}

	public Application getApp(String appId) {
		try {
			JsonDocument doc = appsBucket.get(appId);
			JsonObject content = doc.content();
			String appName = content.getString("appName");
			String appDesc = content.getString("appDesc");
			Application app = new Application(appId, appName, appDesc);
			// System.out.println(appToJson(app));
			return app;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return null;
		}
	}

	public ArrayList<Application> getApps() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean createSensor(Sensor sensor) {
		try {
			JsonObject content = JsonObject.empty().put("appId", sensor.getAppId())
					.put("sensorId", sensor.getSensorId()).put("sensorName", sensor.getSensorName())
					.put("sensorDesc", sensor.getSensorDesc());
			JsonDocument doc = JsonDocument.create(sensor.getSensorId(), content);
			JsonDocument response = sensorsBucket.insert(doc);
			// System.out.println(sensorToJson(sensor));
			return true;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return false;
		}
	}

	public Sensor getSensor(String sensorId) {
		try {
			JsonDocument doc = sensorsBucket.get(sensorId);
			JsonObject content = doc.content();
			String appId = content.getString("appId");
			String sensorName = content.getString("sensorName");
			String sensorDesc = content.getString("sensorDesc");
			Sensor sensor = new Sensor(appId, sensorId, sensorName, sensorDesc);
			// System.out.println(sensorToJson(sensor));
			return sensor;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return null;
		}
	}

	public ArrayList<Sensor> getSensors(String appId) {
		try {
			sensorsBucket.bucketManager().createN1qlPrimaryIndex(true, false);
			ArrayList<Sensor> sensors = new ArrayList<Sensor>();
			Statement statement = select("appId", "sensorId", "sensorName", "sensorDesc").from(i(sensorsBucketName))
					.where(x("appId").eq(x("$appId")));
			JsonObject placeholderValues = JsonObject.create().put("appId", appId);
			ParameterizedN1qlQuery q = N1qlQuery.parameterized(statement, placeholderValues);
			N1qlQueryResult queryResultParameterized = sensorsBucket.query(q);
			for (N1qlQueryRow row : queryResultParameterized) {
				String sensorId = row.value().getString("sensorId");
				String sensorName = row.value().getString("sensorName");
				String sensorDesc = row.value().getString("sensorDesc");
				Sensor sensor = new Sensor(appId, sensorId, sensorName, sensorDesc);
				// System.out.println(sensorToJson(sensor));
				sensors.add(sensor);
			}
			return sensors;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);

			return null;
		}
	}

	public boolean createMetric(Metric metric) {
		try {
			JsonObject content = JsonObject.empty().put("appId", metric.getAppId())
					.put("sensorId", metric.getSensorId()).put("metricId", metric.getMetricId())
					.put("typeOfData", metric.getTypeOfData()).put("mUnit", metric.getmUnit())
					.put("value", metric.getValue()).put("timestamp", metric.getTimestamp());
			JsonDocument doc = JsonDocument.create(metric.getMetricId(), content);
			JsonDocument response = metricsBucket.insert(doc);
			// System.out.println(metricToJson(metric));
			return true;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return false;
		}
	}

	public ArrayList<Metric> getMetrics(String sensorId) {
		try {
			metricsBucket.bucketManager().createN1qlPrimaryIndex(true, false);
			ArrayList<Metric> metrics = new ArrayList<Metric>();
			Statement statement = select("appId", "sensorId", "metricId", "typeOfData", "mUnit", "value", "timestamp")
					.from(i(metricsBucketName)).where(x("sensorId").eq(x("$sensorId")));
			JsonObject placeholderValues = JsonObject.create().put("sensorId", sensorId);
			ParameterizedN1qlQuery q = N1qlQuery.parameterized(statement, placeholderValues);
			N1qlQueryResult queryResultParameterized = metricsBucket.query(q);
			for (N1qlQueryRow row : queryResultParameterized) {
				String appId = row.value().getString("appId");
				String metricId = row.value().getString("metricId");
				String typeOfData = row.value().getString("typeOfData");
				String mUnit = row.value().getString("mUnit");
				String value = row.value().getString("value");
				long timestamp = Long.valueOf(row.value().getString("timestamp"));
				Metric metric = new Metric(appId, sensorId, metricId, typeOfData, mUnit, value, timestamp);
				// System.out.println(metricToJson(metric));
				metrics.add(metric);
			}
			return metrics;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return null;
		}
	}

	public boolean insertMeasurement(Metric metric) {
		try {
			JsonObject content = JsonObject.empty().put("appId", metric.getAppId())
					.put("sensorId", metric.getSensorId()).put("metricId", metric.getMetricId())
					.put("typeOfData", metric.getTypeOfData()).put("mUnit", metric.getmUnit())
					.put("value", metric.getValue()).put("timestamp", metric.getTimestamp());
			JsonDocument doc = JsonDocument.create(metric.getMetricId() + "$" + metric.getTimestamp(), content);
			JsonDocument response = measurementsBucket.insert(doc);
			// System.out.println(metricToJson(metric));
			return true;
		} catch (Exception e) {
			JsonObject responseData = JsonObject.empty().put("success", false)
					.put("failure", "There was an error creating account").put("exception", e.getMessage());
			System.out.println(responseData);
			return false;
		}
	}

	public boolean insertMeasurements(ArrayList<Metric> metric) {
		// TODO Auto-generated method stub
		return false;
	}

	public ArrayList<Metric> getMeasurementsMetricFromTo(String metricId, long date1, long date2) {
		measurementsBucket.bucketManager().createN1qlPrimaryIndex(true, false);
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		Statement statement = select("appId", "sensorId", "metricId", "typeOfData", "mUnit", "value", "timestamp")
				.from(i(measurementsBucketName))
				.where((x("appId").eq(x("$appId"))).and(x("timestamp").gt(x("$timestamp"))));
		JsonObject placeholderValues = JsonObject.create().put("metricId", metricId).put("timestamp", date1);
		ParameterizedN1qlQuery q = N1qlQuery.parameterized(statement, placeholderValues);
		N1qlQueryResult queryResultParameterized = measurementsBucket.query(q);
		for (N1qlQueryRow row : queryResultParameterized) {
			// System.out.println(row);
			String appId = row.value().getString("appId");
			String sensorId = row.value().getString("sensorId");
			String typeOfData = row.value().getString("typeOfData");
			String mUnit = row.value().getString("mUnit");
			String value = row.value().getString("value");
			long timestamp = Long.valueOf(row.value().getString("timestamp"));
			Metric metric = new Metric(appId, sensorId, metricId, typeOfData, mUnit, value, timestamp);
			// System.out.println(metricToJson(metric));
			metrics.add(metric);
		}
		return metrics;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		cluster = CouchbaseCluster.create("172.16.4.15");

		// appsBucket = cluster.openBucket(appsBucketName);
		// sensorsBucket = cluster.openBucket(sensorsBucketName);
		// metricsBucket = cluster.openBucket(metricsBucketName);
		// measurementsBucket = cluster.openBucket(measurementsBucketName);

		appsBucket = cluster.openBucket(measurementsBucketName);
		sensorsBucket = cluster.openBucket(measurementsBucketName);
		metricsBucket = cluster.openBucket(measurementsBucketName);
		measurementsBucket = cluster.openBucket(measurementsBucketName);

		// Application app = new Application(null, "app", "appDesc");
		// createApp(app);
		// // System.out.println("Get app..");
		// getApp(app.getAppId());
		// Sensor sensor = new Sensor(app.getAppId(), null, "sensor1",
		// "appDesc");
		// createSensor(sensor);
		// Sensor sensor2 = new Sensor(app.getAppId(), null, "sensor2",
		// "appDesc");
		// createSensor(sensor2);
		// try {
		// TimeUnit.SECONDS.sleep(4);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("Get sensors..");
		// getSensors(app.getAppId());
		//
		// Metric metric = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "rgreg", "vervre", "", 0);
		// Metric metric4 = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "r4greg", "vervrre", "", 0);
		// createMetric(metric);
		// createMetric(metric4);
		//
		// // System.out.println("Get Metrics");
		// getMetrics(sensor.getSensorId());
		// Metric metric2 = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "rgreg", "vervre", "fdgbfdb", 454894);
		// insertMeasurement(metric2);
		// Metric metric3 = new Metric(app.getAppId(), sensor.getSensorId(),
		// null, "rgmreg", "vervmre", "fdgbfdb",
		// 4548100);
		// insertMeasurement(metric3);
		// // System.out.println("Get Measurements");
		// getMeasurementsMetricFromTo(metric.getMetricId(), 0, 999999999);

	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
		appsBucket.close();
		sensorsBucket.close();
		metricsBucket.close();
		measurementsBucket.close();
		cluster.disconnect();

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
