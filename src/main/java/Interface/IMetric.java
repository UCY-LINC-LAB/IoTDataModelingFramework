package Interface;

public interface IMetric {

	public String getAppId();

	public void setAppId(String appId);

	public String getSensorId();

	public void setSensorId(String sensorId);

	public String getTypeOfData();

	public void setTypeOfData(String typeOfData);

	public String getmUnit();

	public void setmUnit(String mUnit);

	public String getValue();

	public void setValue(String value);

	public long getTimestamp();

	public void setTimestamp(long timestamp);

}
