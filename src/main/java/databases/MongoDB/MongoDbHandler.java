package databases.MongoDB;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lte;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.MySQL.MySqlDbHandler;

public class MongoDbHandler implements IDbHandler {

	private static MongoClient mongoClient;
	private static MongoDatabase database;
	private MongoCollection<Document> appsCollection;
	private MongoCollection<Document> sensorsCollection;
	private MongoCollection<Document> metricsCollection;
	private MongoCollection<Document> measurementsCollection;

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;
	private GsonBuilder builder = new GsonBuilder();
	private Gson gson = builder.create();

	public MongoDbHandler() {
		if (host == null) {
			readProperties();
		}
		if (mongoClient == null) {
			connectToDb(host, port, dbname, user, password);

		}
		if (appsCollection == null) {
			appsCollection = database.getCollection("Applications");
			sensorsCollection = database.getCollection("Sensors");
			metricsCollection = database.getCollection("Metrics");
			measurementsCollection = database.getCollection("Measurements");
		}

	}

	public boolean createApp(Application app) {
		// TODO Auto-generated method stub
		String json = appToJson(app);
		Document doc = Document.parse(json);
		appsCollection.insertOne(doc);
		return true;
	}

	public Application getApp(String appId) {
		// TODO Auto-generated method stub
		Document doc = appsCollection.find(eq("appId", appId)).first();
		Application app = new Application(doc.getString("appId"), doc.getString("appName"), doc.getString("appDesc"));
		appToJson(app);
		return app;
	}

	public ArrayList<Application> getApps() {
		// TODO Auto-generated method stub
		ArrayList<Application> apps = new ArrayList<Application>();
		MongoCursor<Document> cursor = appsCollection.find().iterator();
		try {
			while (cursor.hasNext()) {
				Document doc = cursor.next();
				Application app = new Application(doc.getString("appId"), doc.getString("appName"),
						doc.getString("appDesc"));
				apps.add(app);
				appToJson(app);

			}
		} finally {
			cursor.close();
		}
		return apps;
	}

	public boolean createSensor(Sensor sensor) {
		// TODO Auto-generated method stub
		String json = sensorToJson(sensor);
		Document doc = Document.parse(json);
		sensorsCollection.insertOne(doc);
		return true;
	}

	public Sensor getSensor(String sensorId) {
		// TODO Auto-generated method stub
		Document doc = sensorsCollection.find(eq("sensorId", sensorId)).first();
		Sensor sensor = new Sensor(doc.getString("appId"), doc.getString("sensorId"), doc.getString("sensorName"),
				doc.getString("sensorDesc"));
		sensorToJson(sensor);
		return sensor;
	}

	public ArrayList<Sensor> getSensors(String appId) {
		// TODO Auto-generated method stub
		ArrayList<Sensor> sensors = new ArrayList<Sensor>();
		MongoCursor<Document> cursor = sensorsCollection.find(eq("sensorId", appId)).iterator();
		try {
			while (cursor.hasNext()) {
				Document doc = cursor.next();
				Sensor sensor = new Sensor(doc.getString("appId"), doc.getString("sensorId"),
						doc.getString("sensorName"), doc.getString("sensorDesc"));
				sensors.add(sensor);
				sensorToJson(sensor);

			}
		} finally {
			cursor.close();
		}
		return sensors;
	}

	public boolean createMetric(Metric metric) {
		// TODO Auto-generated method stub
		String json = metricToJson(metric);
		Document doc = Document.parse(json);
		metricsCollection.insertOne(doc);
		return true;
	}

	public ArrayList<Metric> getMetrics(String sensorId) {
		// TODO Auto-generated method stub
		final ArrayList<Metric> metrics = new ArrayList<Metric>();
		// now use a range query to get a larger subset
		Block<Document> printBlock = new Block<Document>() {
			public void apply(final Document doc) {
				Metric metric = new Metric(doc.getString("appId"), doc.getString("sensorId"), doc.getString("metricId"),
						doc.getString("typeOfData"), doc.getString("mUnit"), doc.getString("value"),
						doc.getLong("timestamp"));
				metricToJson(metric);
				metrics.add(metric);
			}
		};
		metricsCollection.find(gt("sensorId", sensorId)).forEach(printBlock);
		return metrics;
	}

	public boolean insertMeasurement(Metric metric) {
		// TODO Auto-generated method stub
		String json = metricToJson(metric);
		Document doc = Document.parse(json);
		doc.put("timestamp", metric.getTimestamp());
		measurementsCollection.insertOne(doc);
		return true;
	}

	public boolean insertMeasurements(ArrayList<Metric> metric) {
		// TODO Auto-generated method stub
		List<Document> docs = new ArrayList<Document>();
		for (int i = 0; i < metric.size(); i++) {
			String json = metricToJson(metric.get(i));
			Document doc = Document.parse(json);
			doc.put("timestamp", metric.get(i).getTimestamp());
			docs.add(doc);
		}
		measurementsCollection.insertMany(docs);
		return true;
	}

	public ArrayList<Metric> getMeasurementsMetricFromTo(String metricId, long date1, long date2) {
		// TODO Auto-generated method stub
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		Block<Document> printBlock = new Block<Document>() {
			public void apply(final Document doc) {
				Metric metric = new Metric(doc.getString("appId"), doc.getString("sensorId"), doc.getString("metricId"),
						doc.getString("typeOfData"), doc.getString("mUnit"), doc.getString("value"),
						doc.getLong("timestamp"));
				metrics.add(metric);
			}
		};
		if (date1 >= date2) {
			measurementsCollection.find(and(eq("metricId", metricId), gt("timestamp", 0), lte("timestamp", 99999)))
					.forEach(printBlock);
		}
		if (date1 <= date2) {
			measurementsCollection.find(and(gt("timestamp", date1), lte("timestamp", date2)));
		}

		return metrics;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {

		// if ((user != null) || (user != "")) {
		// MongoClientOptions.Builder options = MongoClientOptions.builder();
		// options.socketKeepAlive(true);
		// String url = "mongodb://" + user + ":" + pass + "@" + host + ":" +
		// port + "/?authSource=admin";
		// MongoClientURI uri = new MongoClientURI(url);
		// this.mongoClient = new MongoClient(uri);
		// this.database = mongoClient.getDatabase(db);
		// }

		MongoClientOptions options = MongoClientOptions.builder().threadsAllowedToBlockForConnectionMultiplier(100)
				.connectTimeout(100).socketKeepAlive(true).connectionsPerHost(100).build();
		mongoClient = new MongoClient(host, options);
		database = mongoClient.getDatabase(db);

	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
		mongoClient.close();
		return true;
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

			// load a properties file from class path, inside static method
			prop.load(input);

			// get the property value and print it out
			System.out.println(prop.getProperty("db.host"));
			System.out.println(prop.getProperty("db.user"));
			System.out.println(prop.getProperty("db.password"));
			System.out.println(prop.getProperty("db.port"));
			System.out.println(prop.getProperty("db.name"));

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
