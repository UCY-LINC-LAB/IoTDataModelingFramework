/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package myservice.mynamespace.data;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;

import beans.Application;
import beans.Metric;
import beans.Sensor;
import databases.Cassandra.CassandraDbHandler;
import databases.MySQL.MySqlDbHandler;
import myservice.mynamespace.service.DemoEdmProvider;
import myservice.mynamespace.util.Util;

public class Storage {

	MySqlDbHandler db;
	//CassandraDbHandler db;
	// static MongoDbHandler db;
	// static CouchBaseDbHandler db;

	private List<Entity> productList;

	public Storage() {
		productList = new ArrayList<Entity>();
		if (db == null) {
			db = new MySqlDbHandler();
			//db = new CassandraDbHandler();
			// db = new MongoDbHandler();
			// db = new CouchBaseDbHandler();
		}
	}

	/* PUBLIC FACADE */

	public EntityCollection readEntitySetData(EdmEntitySet edmEntitySet) throws ODataApplicationException {
		// System.out.println("readEnitySetData");
		// actually, this is only required if we have more than one Entity Sets
		if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
			return getProducts();
		} else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_APPLICATIONS_NAME)) {
			return getApplications();
		} else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_SENSORS_NAME)) {
			return getApplications();
		} else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_METRICS_NAME)) {
			return getApplications();
		}

		return null;
	}

	public Entity readEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams)
			throws ODataApplicationException {
		// System.out.println("Read Entity Data");
		EdmEntityType edmEntityType = edmEntitySet.getEntityType();

		// actually, this is only required if we have more than one Entity Type
		if (edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)) {
			return getProduct(edmEntityType, keyParams);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_APPLICATION_NAME)) {
			return getApplication(edmEntityType, keyParams);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_SENSOR_NAME)) {
			return getSensor(edmEntityType, keyParams);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_METRIC_NAME)) {
			return getSensor(edmEntityType, keyParams);
		}

		return null;
	}

	/* INTERNAL */

	private EntityCollection getProducts() {
		EntityCollection retEntitySet = new EntityCollection();

		for (Entity productEntity : this.productList) {
			retEntitySet.getEntities().add(productEntity);
		}
		return retEntitySet;
	}

	private EntityCollection getApplications() {
		ArrayList<Application> apps = db.getApps();
		EntityCollection retEntitySet = new EntityCollection();

		for (int i = 0; i < apps.size(); i++) {
			Application app = apps.get(i);
			Entity e1 = new Entity().addProperty(new Property(null, "appId", ValueType.PRIMITIVE, app.getAppId()))
					.addProperty(new Property(null, "appName", ValueType.PRIMITIVE, app.getName()))
					.addProperty(new Property(null, "appDesc", ValueType.PRIMITIVE, app.getDesc()));
			e1.setId(createId("Application", app.getAppId()));
			retEntitySet.getEntities().add(e1);
		}

		return retEntitySet;
	}

	public EntityCollection getMeasurements(String metricId, Long timestamp1, Long timestamp2) {

		ArrayList<Metric> metrics = db.getMeasurementsMetricFromTo(metricId, timestamp1, timestamp2);
		EntityCollection retEntitySet = new EntityCollection();
		for (int i = 0; i < metrics.size(); i++) {
			Metric metric = metrics.get(i);
			Entity entity = new Entity()
					.addProperty(new Property(null, "appId", ValueType.PRIMITIVE, metric.getAppId()))
					.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, metric.getSensorId()))
					.addProperty(new Property(null, "metricId", ValueType.PRIMITIVE, metric.getMetricId()))
					.addProperty(new Property(null, "typeOfData", ValueType.PRIMITIVE, metric.getTypeOfData()))
					.addProperty(new Property(null, "mUnit", ValueType.PRIMITIVE, metric.getmUnit()))
					.addProperty(new Property(null, "value", ValueType.PRIMITIVE, metric.getValue()))
					.addProperty(new Property(null, "timestamp", ValueType.PRIMITIVE, metric.getTimestamp()));
			entity.setId(createId("Metric", metric.getMetricId()));
			retEntitySet.getEntities().add(entity);

		}

		return retEntitySet;
	}

	private Entity getProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams)
			throws ODataApplicationException {

		// System.out.println(keyParams.get(0).getName() + " " +
		// keyParams.get(0).getText());
		// the list of entities at runtime
		EntityCollection entitySet = getProducts();

		/* generic approach to find the requested entity */
		Entity requestedEntity = Util.findEntity(edmEntityType, entitySet, keyParams);

		if (requestedEntity == null) {
			// this variable is null if our data doesn't contain an entity for
			// the requested key
			// Throw suitable exception
			throw new ODataApplicationException("Entity for requested key doesn't exist",
					HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
		}

		return requestedEntity;
	}

	private Entity getApplication(EdmEntityType edmEntityType, List<UriParameter> keyParams)
			throws ODataApplicationException {

		String keyname = keyParams.get(0).getName();
		String keytext = keyParams.get(0).getText();

		keytext = keytext.replace("'", "");
		keytext = keytext.replace("\"", "");

		// System.out.println("Get application " + keyname + " " + keytext);

		Application app = db.getApp(keytext);

		if (app == null) {
			// this variable is null if our data doesn't contain an entity for
			// the requested key
			// Throw suitable exception
			throw new ODataApplicationException("Application for requested key doesn't exist",
					HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
		}

		Entity requestedEntity = new Entity()
				.addProperty(new Property(null, "appId", ValueType.PRIMITIVE, app.getAppId()))
				.addProperty(new Property(null, "appName", ValueType.PRIMITIVE, app.getName()))
				.addProperty(new Property(null, "appDescription", ValueType.PRIMITIVE, app.getDesc()));
		requestedEntity.setId(createId("Application", app.getAppId()));

		return requestedEntity;
	}

	public EntityCollection getAppSensors(String keyname, String keytext) throws ODataApplicationException {

		EntityCollection retEntitySet = new EntityCollection();
		// System.out.println("Get app sensors " + keyname + " " + keytext);

		ArrayList<Sensor> sensors = new ArrayList<Sensor>();

		sensors = db.getSensors(keytext);

		if (sensors.size() == 0) {
			// this variable is null if our data doesn't contain an entity for
			// the requested key
			// Throw suitable exception
			throw new ODataApplicationException("Application for requested key doesn't exist",
					HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
		} else {
			for (int i = 0; i < sensors.size(); i++) {
				Sensor sensor = sensors.get(i);
				Entity requestedEntity = new Entity()
						.addProperty(new Property(null, "appId", ValueType.PRIMITIVE, sensor.getAppId()))
						.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, sensor.getSensorId()))
						.addProperty(new Property(null, "sensorName", ValueType.PRIMITIVE, sensor.getSensorName()))
						.addProperty(new Property(null, "sensorDesc", ValueType.PRIMITIVE, sensor.getSensorDesc()));
				requestedEntity.setId(createId("Sensor", sensor.getSensorId()));
				retEntitySet.getEntities().add(requestedEntity);

			}
		}

		return retEntitySet;
	}

	public EntityCollection getSensorMetrics(String keyname, String keytext) throws ODataApplicationException {

		EntityCollection retEntitySet = new EntityCollection();
		// System.out.println("Get metrics for sensors " + keyname + " " +
		// keytext);

		ArrayList<Metric> metrics = new ArrayList<Metric>();

		metrics = db.getMetrics(keytext);

		if (metrics.size() == 0) {
			// this variable is null if our data doesn't contain an entity for
			// the requested key
			// Throw suitable exception
			throw new ODataApplicationException("Metrics for requested key doesn't exist",
					HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
		} else {
			for (int i = 0; i < metrics.size(); i++) {
				Metric metric = metrics.get(i);
				Entity entity = new Entity()
						.addProperty(new Property(null, "appId", ValueType.PRIMITIVE, metric.getAppId()))
						.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, metric.getSensorId()))
						.addProperty(new Property(null, "metricId", ValueType.PRIMITIVE, metric.getMetricId()))
						.addProperty(new Property(null, "typeOfData", ValueType.PRIMITIVE, metric.getTypeOfData()))
						.addProperty(new Property(null, "mUnit", ValueType.PRIMITIVE, metric.getmUnit()))
						.addProperty(new Property(null, "value", ValueType.PRIMITIVE, metric.getValue()))
						.addProperty(new Property(null, "timestamp", ValueType.PRIMITIVE, metric.getTimestamp()));
				entity.setId(createId("Sensor", metric.getSensorId()));
				retEntitySet.getEntities().add(entity);
			}
		}
		return retEntitySet;
	}

	private Entity getSensor(EdmEntityType edmEntityType, List<UriParameter> keyParams)
			throws ODataApplicationException {

		String keyname = keyParams.get(0).getName();
		String keytext = keyParams.get(0).getText();

		keytext = keytext.replace("'", "");
		keytext = keytext.replace("\"", "");

		// System.out.println("Get sensor " + keyname + " " + keytext);

		Sensor sensor = db.getSensor(keytext);

		if (sensor == null) {
			// this variable is null if our data doesn't contain an entity for
			// the requested key
			// Throw suitable exception
			throw new ODataApplicationException("Application for requested key doesn't exist",
					HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
		}

		Entity requestedEntity = new Entity()
				.addProperty(new Property(null, "appId", ValueType.PRIMITIVE, sensor.getAppId()))
				.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, sensor.getSensorId()))
				.addProperty(new Property(null, "sensorName", ValueType.PRIMITIVE, sensor.getSensorName()))
				.addProperty(new Property(null, "sensorDesc", ValueType.PRIMITIVE, sensor.getSensorDesc()));
		requestedEntity.setId(createId("Application", sensor.getSensorId()));

		return requestedEntity;
	}

	/* HELPER */

	@SuppressWarnings("unused")
	private void initSampleData() {

		// add some sample product entities
		final Entity e1 = new Entity().addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 1))
				.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Notebook Basic 15"))
				.addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
						"Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"));
		e1.setId(createId("Products", 1));
		productList.add(e1);

		final Entity e2 = new Entity().addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 2))
				.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "1UMTS PDA"))
				.addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
						"Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"));
		e2.setId(createId("Products", 2));
		productList.add(e2);

		final Entity e3 = new Entity().addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 3))
				.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Ergo Screen"))
				.addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
						"19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"));
		e3.setId(createId("Products", 3));
		productList.add(e3);
	}

	private URI createId(String entitySetName, Object id) {
		try {
			return new URI(entitySetName + "(" + String.valueOf(id) + ")");
		} catch (URISyntaxException e) {
			throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
		}
	}

	public Entity createEntityData(EdmEntitySet edmEntitySet, Entity entityToCreate) {

		EdmEntityType edmEntityType = edmEntitySet.getEntityType();

		// actually, this is only required if we have more than one Entity Type
		if (edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)) {
			return createProduct(edmEntityType, entityToCreate);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_APPLICATION_NAME)) {
			return createApplication(edmEntityType, entityToCreate);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_SENSOR_NAME)) {
			return createSensor(edmEntityType, entityToCreate);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_METRIC_NAME)) {
			return createMetric(edmEntityType, entityToCreate);
		} else if (edmEntityType.getName().equals(DemoEdmProvider.ET_MEASUREMENT_NAME)) {
			return insertMeasurement(edmEntityType, entityToCreate);
		}

		return null;
	}

	private Entity createApplication(EdmEntityType edmEntityType, Entity entity) {

		String appId = entity.getProperty("appId").getValue().toString();
		Application app = null;
		if (appId != null) {
			app = new Application(appId, entity.getProperty("appName").getValue().toString(),
					entity.getProperty("appDesc").getValue().toString());
		} else {
			app = new Application(null, entity.getProperty("appName").getValue().toString(),
					entity.getProperty("appDesc").getValue().toString());
		}
		// System.out.println(db.appToJson(app));
		if (db.createApp(app)) {
			entity = new Entity().addProperty(new Property(null, "appId", ValueType.PRIMITIVE, app.getAppId()))
					.addProperty(new Property(null, "appName", ValueType.PRIMITIVE, app.getName()))
					.addProperty(new Property(null, "appDesc", ValueType.PRIMITIVE, app.getDesc()));
			entity.setId(createId("Application", app.getAppId()));
		} else {
			entity = null;
		}
		return entity;
	}

	private Entity createSensor(EdmEntityType edmEntityType, Entity entity) {
		String appId = entity.getProperty("appId").getValue().toString();
		String sensorName = entity.getProperty("sensorName").getValue().toString();
		String sensorDescription = entity.getProperty("sensorDesc").getValue().toString();

		String sensorId = entity.getProperty("sensorId").getValue().toString();
		Sensor sensor = null;
		if (sensorId != null) {
			sensor = new Sensor(appId, sensorId, sensorName, sensorDescription);
		} else {
			sensor = new Sensor(appId, null, sensorName, sensorDescription);
		}
		if (db.createSensor(sensor)) {
			entity = new Entity().addProperty(new Property(null, "appId", ValueType.PRIMITIVE, sensor.getAppId()))
					.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, sensor.getSensorId()))
					.addProperty(new Property(null, "sensorName", ValueType.PRIMITIVE, sensor.getSensorName()))
					.addProperty(new Property(null, "sensorDesc", ValueType.PRIMITIVE, sensor.getSensorDesc()));

			entity.setId(createId("Sensor", sensor.getSensorId()));
		} else {
			entity = null;
		}
		return entity;
	}

	private Entity createMetric(EdmEntityType edmEntityType, Entity entity) {
		String appId = entity.getProperty("appId").getValue().toString();
		String sensorId = entity.getProperty("sensorId").getValue().toString();
		String typeOfData = entity.getProperty("typeOfData").getValue().toString();
		String mUnit = entity.getProperty("mUnit").getValue().toString();

		Metric metric = new Metric(appId, sensorId, null, typeOfData, mUnit, null, 0);
		if (db.createMetric(metric)) {
			entity = new Entity().addProperty(new Property(null, "appId", ValueType.PRIMITIVE, metric.getAppId()))
					.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, metric.getSensorId()))
					.addProperty(new Property(null, "metricId", ValueType.PRIMITIVE, metric.getMetricId()))
					.addProperty(new Property(null, "typeOfData", ValueType.PRIMITIVE, metric.getTypeOfData()))
					.addProperty(new Property(null, "mUnit", ValueType.PRIMITIVE, metric.getmUnit()))
					.addProperty(new Property(null, "value", ValueType.PRIMITIVE, metric.getValue()))
					.addProperty(new Property(null, "timestamp", ValueType.PRIMITIVE, metric.getTimestamp()));
			entity.setId(createId("Metric", metric.getMetricId()));
			// System.out.println("Metric insert");
		} else {
			entity = null;
		}
		return entity;
	}

	private Entity insertMeasurement(EdmEntityType edmEntityType, Entity entity) {
		String appId = entity.getProperty("appId").getValue().toString();
		String sensorId = entity.getProperty("sensorId").getValue().toString();
		String metricId = entity.getProperty("metricId").getValue().toString();
		String typeOfData = entity.getProperty("typeOfData").getValue().toString();
		String mUnit = entity.getProperty("mUnit").getValue().toString();
		String value = entity.getProperty("value").getValue().toString();
		String timestamp = entity.getProperty("timestamp").getValue().toString();

		Metric metric = new Metric(appId, sensorId, metricId, typeOfData, mUnit, value, Integer.valueOf(timestamp));
		if (db.insertMeasurement(metric)) {
			entity = new Entity().addProperty(new Property(null, "appId", ValueType.PRIMITIVE, metric.getAppId()))
					.addProperty(new Property(null, "sensorId", ValueType.PRIMITIVE, metric.getSensorId()))
					.addProperty(new Property(null, "metricId", ValueType.PRIMITIVE, metric.getMetricId()))
					.addProperty(new Property(null, "typeOfData", ValueType.PRIMITIVE, metric.getTypeOfData()))
					.addProperty(new Property(null, "mUnit", ValueType.PRIMITIVE, metric.getmUnit()))
					.addProperty(new Property(null, "value", ValueType.PRIMITIVE, metric.getValue()))
					.addProperty(new Property(null, "timestamp", ValueType.PRIMITIVE, metric.getTimestamp()));
			entity.setId(createId("Metric", metric.getMetricId()));
		} else {
			entity = null;
		}

		return entity;
	}

	private Entity createProduct(EdmEntityType edmEntityType, Entity entity) {
		// the ID of the newly created product entity is generated automatically
		int newId = 1;
		while (productIdExists(newId)) {
			newId++;
		}

		Property idProperty = entity.getProperty("ID");
		if (idProperty != null) {
			idProperty.setValue(ValueType.PRIMITIVE, Integer.valueOf(newId));
		} else {
			// as of OData v4 spec, the key property can be omitted from the
			// POST request body
			entity.getProperties().add(new Property(null, "ID", ValueType.PRIMITIVE, newId));
		}
		entity.setId(createId("Products", newId));
		this.productList.add(entity);

		return entity;
	}

	private boolean productIdExists(int id) {
		for (Entity entity : this.productList) {
			Integer existingID = (Integer) entity.getProperty("ID").getValue();
			if (existingID.intValue() == id) {
				return true;
			}
		}
		return false;
	}

	public boolean closeCon() {
		return db.closeConnection();
	}

}
