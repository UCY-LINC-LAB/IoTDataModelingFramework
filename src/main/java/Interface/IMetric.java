package Interface;

public interface IMetric {
	// Get application's id
	public String getAppId();

	// Set application's id
	public boolean setAppId(String appId);

	// Get sensor's id
	public String getSensorId();

	// Set sensod's id
	public boolean setSensorId(String sensorId);

	// Get the type of data (One dimension, Multidimensional)
	public String getTypeOfData();

	// Set type of data
	public boolean setTypeOfData(String typeOfData);

	// Get measurement unit (C,km/h)
	public String getmUnit();

	// Set measurement unit
	public boolean setmUnit(String mUnit);

	// Get current value, measurement
	public String getValue();

	// Set value
	public boolean setValue(String value);

	// Get latest timestamp (unix)
	public long getTimestamp();

	// Set timestamp
	public boolean setTimestamp(long timestamp);

}
