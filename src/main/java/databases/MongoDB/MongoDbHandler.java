package databases.MongoDB;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import Interface.IDbHandler;
import beans.Application;
import beans.Metric;
import beans.Sensor;

public class MongoDbHandler implements IDbHandler {

	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> appsCollection;
	MongoCollection<Document> sensorsCollection;
	MongoCollection<Document> metricsCollection;
	MongoCollection<Document> measurementsCollection;

	private static String host;
	private static String dbname;
	private static String port;
	private static String user;
	private static String password;
	private static GsonBuilder builder = new GsonBuilder();
	private static Gson gson = builder.create();

	public MongoDbHandler() {
		readProperties();
		connectToDb(host, port, dbname, user, password);
		appsCollection = database.getCollection("Applications");
		sensorsCollection = database.getCollection("Sensors");
		metricsCollection = database.getCollection("Metrics");
		measurementsCollection = database.getCollection("Measurements");

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

	public ArrayList<Sensor> getSensors() {
		// TODO Auto-generated method stub
		ArrayList<Sensor> sensors = new ArrayList<Sensor>();
		MongoCursor<Document> cursor = sensorsCollection.find().iterator();
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
		if (date1 > date2) {
			measurementsCollection.find(and(gt("timestamp", date2), lte("timestamp", date1))).forEach(printBlock);
		}
		if (date1 < date2) {
			measurementsCollection.find(and(gt("timestamp", date1), lte("timestamp", date2))).forEach(printBlock);
		}
		for (int i = 0; i < metrics.size(); i++) {
			System.out.println(metricToJson(metrics.get(i)));
		}

		return metrics;
	}

	public void connectToDb(String host, String port, String db, String user, String pass) {
		MongoClientOptions.Builder options = MongoClientOptions.builder();
		options.socketKeepAlive(true);
		String url = "mongodb://" + user + ":" + pass + "@" + host + ":" + port + "/?authSource=admin";
		MongoClientURI uri = new MongoClientURI(url);
		mongoClient = new MongoClient(uri);
		database = mongoClient.getDatabase(db);

	}

	public boolean closeConnection() {
		// TODO Auto-generated method stub
		mongoClient.close();
		return true;
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
