package beans;

import Interface.IMetric;

public class Metric implements IMetric {
	private String appId;
	private String sensorId;
	private String metricId;
	private String typeOfData;
	private String mUnit;
	private String value;
	private long timestamp;

	public Metric(String appId, String sensorId, String metricId, String typeOfData, String mUnit, String value,
			long timestamp) {
		this.appId = appId;
		this.sensorId = sensorId;
		this.typeOfData = typeOfData;
		this.mUnit = mUnit;
		this.value = value;
		this.timestamp = timestamp;
		if (metricId == null || metricId.compareTo("") == 0) {
			this.metricId = sensorId + "$" + mUnit;
		} else {
			this.metricId = metricId;
		}
	}

	public String getAppId() {
		return appId;
	}

	public boolean setAppId(String appId) {
		this.appId = appId;
		return true;
	}

	public String getSensorId() {
		return sensorId;
	}

	public boolean setSensorId(String sensorId) {
		this.sensorId = sensorId;
		return true;
	}

	public String getMetricId() {
		return metricId;
	}

	public void setMetricId(String metricId) {
		this.metricId = metricId;
	}

	public String getTypeOfData() {
		return typeOfData;
	}

	public boolean setTypeOfData(String typeOfData) {
		this.typeOfData = typeOfData;
		return true;
	}

	public String getmUnit() {
		return mUnit;
	}

	public boolean setmUnit(String mUnit) {
		this.mUnit = mUnit;
		return true;
	}

	public String getValue() {
		return value;
	}

	public boolean setValue(String value) {
		this.value = value;
		return true;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return true;
	}
}
