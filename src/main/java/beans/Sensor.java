package beans;

import java.util.UUID;
import Interface.ISensor;

public class Sensor implements ISensor {

	private String appId;
	private String sensorId;
	private String sensorName;
	private String sensorDesc;

	public Sensor(String appId, String sensorId, String sensorName, String sensorDesc) {
		this.appId = appId;
		this.sensorName = sensorName;
		this.sensorDesc = sensorDesc;
		if (sensorId == null) {
			this.sensorId = generateUuid();
		} else {
			this.sensorId = sensorId;
		}
	}

	public String getAppId() {
		return appId;
	}

	public boolean setAppId(String appId) {
		this.appId = appId;
		return true;
	}

	public String getSensorName() {
		return sensorName;
	}

	public boolean setSensorName(String sensorName) {
		this.sensorName = sensorName;
		return true;
	}

	public String getSensorDesc() {
		return sensorDesc;
	}

	public boolean setSensorDesc(String sensorDesc) {
		this.sensorDesc = sensorDesc;
		return true;
	}

	public String getSensorId() {
		return sensorId;
	}

	private static String generateUuid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

}
