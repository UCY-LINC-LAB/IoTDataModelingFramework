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
		if (appId == null || appId.compareTo("") == 0)
			this.appId = generateUuid();
		else
			this.appId = appId;
	}

	public String getName() {
		return appName;
	}

	public void setName(String name) {
		this.appName = name;
	}

	public String getDesc() {
		return appDesc;
	}

	public void setDesc(String desc) {
		this.appDesc = desc;
	}

	public String getAppId() {
		return appId;
	}

	private static String generateUuid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

}
