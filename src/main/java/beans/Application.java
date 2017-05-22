package beans;

import java.util.UUID;
import Interface.IApplication;

public class Application implements IApplication {

	private String appId;
	private String appName;
	private String appDesc;

	public Application(String appId, String name, String desc) {
		this.appName = name;
		this.appDesc = desc;
		// if (appId == null) {
		// this.appId = generateUuid();
		// } else {
		// this.appId = appId;
		// }

		try {
			// UUID uuid = UUID.fromString(appId);
			// appId = uuid.toString();
			this.appId = appId;
			// do something
		} catch (IllegalArgumentException exception) {
			// handle the case where string is not valid UUID
			this.appId = generateUuid();
		}

	}

	public String getName() {
		return appName;
	}

	public boolean setName(String name) {
		this.appName = name;
		return true;
	}

	public String getDesc() {
		return appDesc;
	}

	public boolean setDesc(String desc) {
		this.appDesc = desc;
		return true;
	}

	public String getAppId() {
		return appId;
	}

	private static String generateUuid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

}
