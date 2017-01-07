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

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getSensorId() {
		return sensorId;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
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

	public void setTypeOfData(String typeOfData) {
		this.typeOfData = typeOfData;
	}

	public String getmUnit() {
		return mUnit;
	}

	public void setmUnit(String mUnit) {
		this.mUnit = mUnit;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
