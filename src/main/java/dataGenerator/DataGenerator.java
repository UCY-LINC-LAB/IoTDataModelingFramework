package dataGenerator;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import Logger.Logger;
import databases.CouchBase.CouchBaseDbHandler;

public class DataGenerator {

	// private boolean enableRead = false;
	// private boolean getDelay = false;
	public static int reqPerSec;
	public static int sumRps;
	private static int numberOfThreads;
	public static int sensors;
	private static int streamNum;
	public static String logsLocation;
	public static String logsName;

	public static ArrayList<Integer> rtt = new ArrayList<Integer>();
	public static ArrayList<Integer> rttDelay = new ArrayList<Integer>();

	static ThreadPoolExecutor executorPool;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		CouchBaseDbHandler db = new CouchBaseDbHandler();

		if (args.length > 5) {
			sensors = Integer.parseInt(args[0]);
			streamNum = Integer.parseInt(args[1]);
			int streamTime = Integer.parseInt(args[2]);
			int periodTime = Integer.parseInt(args[3]);
			int immStart = Integer.parseInt(args[4]);
			logsLocation = args[5];
			logsName = args[6];

			Date date = new Date();

			Logger.writeDelay(date.toString());
			Logger.writeInsertTime(date.toString());
			Logger.writeQuery(date.toString());
			Logger.writeRps(date.toString());

			// RejectedExecutionHandler implementation
			RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
			// Get the ThreadFactory implementation to use
			ThreadFactory threadFactory = Executors.defaultThreadFactory();
			// creating the ThreadPoolExecutor
			// after tests it was the maximum number of threads java could run
			numberOfThreads = 5000;
			executorPool = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 10, TimeUnit.MINUTES,
					new ArrayBlockingQueue<Runnable>(numberOfThreads), threadFactory, rejectionHandler);

			// start the monitoring thread
			MyMonitorThread monitor = new MyMonitorThread(executorPool, 3);
			Thread monitorThread = new Thread(monitor);
			monitorThread.start();

			ArrayList<Runnable> streamerList = new ArrayList<Runnable>();

			startRpsCounter();
			if (streamTime > 0)
				startTimer(streamTime);

			for (int i = 0; i < sensors; i++) {
				Runnable streamer = new DataStream(0, i, streamNum, periodTime);
				if (immStart >= 1) {
					executorPool.execute(streamer);
					// (new Thread(streamer)).start();
				} else {
					streamerList.add(streamer);
				}
			}
			if (immStart <= 0) {
				for (int i = 0; i < sensors; i++) {
					executorPool.execute(streamerList.get(i));
				}
			}

		} else {
			System.out.println(
					"Arguments needed! Sensors,Stream Number,Stream Time,Period Time,Fast Start(0 or 1),Logs location");
		}
	}

	public static ThreadPoolExecutor returnExecutor() {
		return executorPool;
	}

	public static void startRpsCounter() {
		reqPerSec = 0;
		sumRps = 0;
		executorPool.execute(new Runnable() {
			public void run() {
				int time = 0;
				while (true) {
					try {
						TimeUnit.SECONDS.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (reqPerSec > 0) {
						time++;
						System.out.println(reqPerSec);
						sumRps += reqPerSec;
						// writes RPS to file
						// Logger.writeRps(String.valueOf(reqPerSec));
						reqPerSec = 0;
						if (reqPerSec == 0) {
							Logger.writeRps("Number of Requests " + sumRps + " Average RPS "
									+ String.valueOf((int) sumRps / time));
						}
					}

					if (rtt.size() == sensors || rttDelay.size() == sensors) {
						int sum = 0;
						for (int i = 0; i < rtt.size(); i++) {
							sum += rtt.get(i);
						}
						String toWrite = "Average of All Sensors = " + (sum / sensors) + " ms";
						Logger.writeInsertTime(toWrite);
						System.out.println(toWrite);
						rtt.removeAll(rtt);
					}

					if (rttDelay.size() == sensors || rtt.size() == sensors) {
						int sum = 0;
						for (int i = 0; i < rttDelay.size(); i++) {
							sum += rttDelay.get(i);
						}
						String toWrite = "Average of All Sensors delay = " + (sum / sensors) + " ms";
						Logger.writeDelay(toWrite);
						System.out.println(toWrite);
						rttDelay.removeAll(rttDelay);

					}

				}
			}
		});
	}

	public static void startTimer(int streamTime) {
		System.out.println(Thread.currentThread().getName() + " Start executor shutdown!");
		try {
			TimeUnit.MINUTES.sleep(streamTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			System.out.println("attempt to shutdown executor");
			executorPool.shutdown();
			executorPool.awaitTermination(streamTime, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.err.println("tasks interrupted");
		} finally {
			if (!executorPool.isTerminated()) {
				System.err.println("cancel non-finished tasks");
			}
			executorPool.shutdownNow();
			System.out.println("shutdown finished");
			System.exit(1);
		}
	}

}