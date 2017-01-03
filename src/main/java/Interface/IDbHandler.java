package Interface;

import beans.Application;
import beans.Metric;
import beans.Sensor;

public interface IDbHandler {

	public void createApp(Application app);

	public void createSensor(Sensor sensor);

	public void createMunit(Metric munit);

	public void insertMetric(Metric data);

	public void execStmnt(String stmt);

	public void connectToDb(String host, String port, String db, String user, String pass);

	public void closeConnection();

	public boolean status();

}
