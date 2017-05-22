package HttpHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import Logger.Logger;
import beans.Metric;
import dataGenerator.DataGenerator;
import databases.MySQL.MySqlDbHandler;

public class HttpClass {

	private String address;
	private String uri;
	private CloseableHttpClient httpClient;
	private PoolingHttpClientConnectionManager cm;
	private CloseableHttpResponse response;
	// private static ThreadPoolExecutor executorPool;

	public HttpClass() {
		// cm = new PoolingHttpClientConnectionManager();
		// cm.setMaxTotal(1000000);
		// cm.setDefaultMaxPerRoute(1000000);
		// httpClient = HttpClients.createDefault();

		// httpClient = HttpClients.custom().setConnectionManager(cm).build();

		int timeout = 1000000000;
		RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();
		httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

		address = "172.16.4.28";
		uri = "http://" + address + ":8080/IoTDataModelingFramework/DemoService.svc/";
		// executorPool = DataGenerator.returnExecutor();

	}

	// HTTP POST request
	public boolean sendPost(String object, String data) throws Exception {
		long startTime = System.currentTimeMillis();
		String postUri = uri + object;
		HttpPost httpPost = new HttpPost(postUri);
		StringEntity postingString = new StringEntity(data);
		httpPost.setEntity(postingString);
		httpPost.setHeader("Content-type", "application/json");

		// PostThread thread = new PostThread(httpClient, httpPost);
		// executorPool.execute(thread);

		try {
			response = httpClient.execute(httpPost);
			// In case of error print error message
			if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() > 300) {
				System.out.println(response.getStatusLine() + " " + data);
				return false;
			}
		} catch (IOException | UnsupportedOperationException e) {
			e.printStackTrace();
		} finally {
			httpPost.releaseConnection();
			if (null != response) {
				try {
					HttpEntity entity = response.getEntity();
					EntityUtils.consume(entity);
					response.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		long endTime = System.currentTimeMillis();
		String toWrite = "Start " + startTime + " End " + endTime + " Difference " + (endTime - startTime) + " ms";
		Logger.writeInsertTime(toWrite);

		return true;
	}

	public boolean sendGet(Metric metric) throws Exception {
		long startTime = System.currentTimeMillis();
		String url = "http://localhost:8080/IoTDataModelingFramework/DemoService.svc/Measurements?$filter=metricId%20eq%20%27"
				+ metric.getMetricId() + "%27%20and%20timestamp%20gt%20" + metric.getTimestamp()
				+ "%20and%20timestamp%20lt%20" + metric.getTimestamp();

		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url);
		HttpResponse response = client.execute(request);
		// System.out.println("Response Code : " +
		// response.getStatusLine().getStatusCode());
		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		System.out.println(result.toString());

		if (response.getStatusLine().getStatusCode() == 200) {
			long endTime = System.currentTimeMillis();
			String toWrite = "Start " + startTime + " End " + endTime + " Delay " + (endTime - startTime) + " ms";
			Logger.writeDelay(toWrite);
			return true;
		}
		return false;
	}

	public void closeCon() {
		try {
			if (response != null) {
				HttpEntity entity = response.getEntity();
				EntityUtils.consume(entity);
				response.close();
			}
			if (httpClient != null)
				httpClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static class GetThread extends Thread {

		private final CloseableHttpClient httpClient;
		private final HttpContext context;
		private final HttpGet httpget;

		public GetThread(CloseableHttpClient httpClient, HttpGet httpget) {
			this.httpClient = httpClient;
			this.context = HttpClientContext.create();
			this.httpget = httpget;
		}

		@Override
		public void run() {
			try {
				CloseableHttpResponse response = httpClient.execute(httpget, context);
				try {
					HttpEntity entity = response.getEntity();
				} finally {
					response.close();
				}
			} catch (ClientProtocolException ex) {
				// Handle protocol errors
			} catch (IOException ex) {
				// Handle I/O errors
			}
		}
	}

	static class PostThread extends Thread {

		private final CloseableHttpClient httpClient;
		private final HttpContext context;
		private final HttpPost httpPost;

		public PostThread(CloseableHttpClient httpClient, HttpPost httpPost) {
			this.httpClient = httpClient;
			this.context = HttpClientContext.create();
			this.httpPost = httpPost;
		}

		@Override
		public void run() {
			try {
				CloseableHttpResponse response = httpClient.execute(httpPost, context);
				try {
					HttpEntity entity = response.getEntity();
					EntityUtils.consume(entity);
				} finally {
					response.close();
				}
			} catch (ClientProtocolException ex) {
				// Handle protocol errors
			} catch (IOException ex) {
				// Handle I/O errors
			}
		}
	}

}
