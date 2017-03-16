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
			// try {
			// UUID uuid = UUID.fromString(sensorId);
			// this.sensorId = uuid.toString();
			// // do something
			// } catch (IllegalArgumentException exception) {
			// // handle the case where string is not valid UUID
			// this.sensorId = generateUuid();
			// }
			this.sensorId = sensorId;
		}
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getSensorName() {
		return sensorName;
	}

	public void setSensorName(String sensorName) {
		this.sensorName = sensorName;
	}

	public String getSensorDesc() {
		return sensorDesc;
	}

	public void setSensorDesc(String sensorDesc) {
		this.sensorDesc = sensorDesc;
	}

	public String getSensorId() {
		return sensorId;
	}

	private static String generateUuid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

}
