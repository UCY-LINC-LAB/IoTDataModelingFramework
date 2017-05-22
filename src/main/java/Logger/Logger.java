package Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import dataGenerator.DataGenerator;

public class Logger {

	private static BufferedWriter bwInsert;
	private static FileWriter fwInsert;
	private static BufferedWriter bwDelay;
	private static FileWriter fwDelay;
	private static BufferedWriter bwRps;
	private static FileWriter fwRps;
	private static BufferedWriter bwQuery;
	private static FileWriter fwQuery;

	private static File fileInsert;
	private static File fileDelay;
	private static File fileRps;
	private static File fileQuery;

	private static String home = System.getProperty("user.home");
	private static String path = "/" + DataGenerator.logsLocation + "/";
	private final static String insertTime = "insertTime" + DataGenerator.sensors + DataGenerator.logsName + ".txt";
	private final static String delayTime = "delayTime" + DataGenerator.sensors + DataGenerator.logsName + ".txt";
	private final static String rpsTime = "reqPerSeq" + DataGenerator.sensors + DataGenerator.logsName + ".txt";
	private final static String queryTime = "queryTime" + DataGenerator.sensors + DataGenerator.logsName + ".txt";
	private final static String averageTimes = "averageTimes" + DataGenerator.sensors + DataGenerator.logsName + ".txt";

	public static void writeInsertTime(String data) {
		data = data + "\n";

		try {
			fileInsert = new File(home + path + insertTime);
			if (!fileInsert.getParentFile().exists()) {
				fileInsert.getParentFile().mkdirs();
			}
			// if file doesnt exists, then create it
			if (!fileInsert.exists()) {
				try {
					fileInsert.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			fwInsert = new FileWriter(fileInsert.getAbsoluteFile(), true);
			bwInsert = new BufferedWriter(fwInsert);
			bwInsert.write(data);
			bwInsert.flush();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	public static void writeDelay(String data) {
		data = data + "\n";
		try {
			fileDelay = new File(home + path + delayTime);
			if (!fileDelay.getParentFile().exists()) {
				fileDelay.getParentFile().mkdirs();
			}
			// if file doesnt exists, then create it
			if (!fileDelay.exists()) {
				try {
					fileDelay.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			fwDelay = new FileWriter(fileDelay.getAbsoluteFile(), true);
			bwDelay = new BufferedWriter(fwDelay);
			bwDelay.write(data);
			bwDelay.flush();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	public static void writeRps(String data) {
		data = data + "\n";
		try {
			fileRps = new File(home + path + rpsTime);
			if (!fileRps.getParentFile().exists()) {
				fileRps.getParentFile().mkdirs();
			}
			// if file doesnt exists, then create it
			if (!fileRps.exists()) {
				try {
					fileRps.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			fwRps = new FileWriter(fileRps.getAbsoluteFile(), true);
			bwRps = new BufferedWriter(fwRps);
			bwRps.write(data);
			bwRps.flush();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	public static void writeQuery(String data) {
		data = data + "\n";

		try {
			fileQuery = new File(home + path + queryTime);
			if (!fileQuery.getParentFile().exists()) {
				fileQuery.getParentFile().mkdirs();
			}
			// if file doesnt exists, then create it
			if (!fileQuery.exists()) {
				try {
					fileQuery.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			fwQuery = new FileWriter(fileQuery.getAbsoluteFile(), true);
			bwQuery = new BufferedWriter(fwQuery);
			bwQuery.write(data);
			bwQuery.flush();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	public static void closeCon() {
		try {
			if (bwQuery != null)
				bwQuery.close();
			if (fwRps != null)
				fwRps.close();
			if (bwRps != null)
				bwRps.close();
			if (fwDelay != null)
				fwDelay.close();
			if (bwDelay != null)
				bwDelay.close();
			if (fwInsert != null)
				fwInsert.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static void averageTimes(String data) {
		data = data + "\n";

		try {
			fileInsert = new File(home + path + averageTimes);
			if (!fileInsert.getParentFile().exists()) {
				fileInsert.getParentFile().mkdirs();
			}
			// if file doesnt exists, then create it
			if (!fileInsert.exists()) {
				try {
					fileInsert.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			fwInsert = new FileWriter(fileInsert.getAbsoluteFile(), true);
			bwInsert = new BufferedWriter(fwInsert);
			bwInsert.write(data);
			bwInsert.flush();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

}
