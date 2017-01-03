import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.MySQL.MySqlDbHandler;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Application app = new Application("app1", "sdfsd");
		Sensor sensor = new Sensor(app.getAppId(), "sensor1", "desc");
		MySqlDbHandler db = new MySqlDbHandler();
		db.connectToDb("localhost", "3306", "ADE", "root", "");
		db.createApp(app);
		db.createSensor(sensor);
		Metric metric = new Metric(app.getAppId(), sensor.getSensorId(), "typeOfData", "C", null, 0);
		db.createMunit(metric);
		metric.setValue("150");
		metric.setTimestamp(1561564562);
		db.insertMetric(metric);

	}

}
