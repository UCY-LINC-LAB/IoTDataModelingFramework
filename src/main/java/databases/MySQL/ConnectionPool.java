package databases.MySQL;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

public class ConnectionPool {

	private GenericObjectPool connectionPool = null;

	public DataSource setUp(String driver, String url, String username, String password) throws Exception {
		// Load JDBC Driver class.
		Class.forName(driver).newInstance();

		// Creates an instance of GenericObjectPool that holds our
		// pool of connections object.
		connectionPool = new GenericObjectPool();
		connectionPool.setMaxActive(1000);
		connectionPool.setMaxWait(-1);
		connectionPool.setMaxIdle(-1);
		connectionPool.setMinIdle(-1);
		connectionPool.setSoftMinEvictableIdleTimeMillis(-1);
		connectionPool.setTimeBetweenEvictionRunsMillis(-1);
		connectionPool.setNumTestsPerEvictionRun(10);
		connectionPool.setTestOnBorrow(true);
		connectionPool.setTestWhileIdle(true);
		connectionPool.setTestOnReturn(false);

		// Creates a connection factory object which will be use by
		// the pool to create the connection object. We passes the
		// JDBC url info, username and password.
		ConnectionFactory cf = new DriverManagerConnectionFactory(url, username, password);

		// Creates a PoolableConnectionFactory that will wraps the
		// connection object created by the ConnectionFactory to add
		// object pooling functionality.
		PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, connectionPool, null, null, false, true);
		return new PoolingDataSource(connectionPool);
	}

	public GenericObjectPool getConnectionPool() {
		return connectionPool;
	}

	public void printStatus() {
		System.out.println("Max   : " + getConnectionPool().getMaxActive() + "; " + "Active: "
				+ getConnectionPool().getNumActive() + "; " + "Idle  : " + getConnectionPool().getNumIdle());
	}
}