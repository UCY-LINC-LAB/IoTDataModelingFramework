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
package myservice.mynamespace.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

public class DemoEdmProvider extends CsdlAbstractEdmProvider {

	// Service Namespace
	public static final String NAMESPACE = "OData.Demo";

	// EDM Container
	public static final String CONTAINER_NAME = "Container";
	public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE, CONTAINER_NAME);

	// Entity Types Names
	public static final String ET_PRODUCT_NAME = "Product";
	public static final FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(NAMESPACE, ET_PRODUCT_NAME);
	public static final String ET_APPLICATION_NAME = "Application";
	public static final FullQualifiedName ET_APPLICATION_FQN = new FullQualifiedName(NAMESPACE, ET_APPLICATION_NAME);
	public static final String ET_SENSOR_NAME = "Sensor";
	public static final FullQualifiedName ET_SENSOR_FQN = new FullQualifiedName(NAMESPACE, ET_SENSOR_NAME);
	public static final String ET_METRIC_NAME = "Metric";
	public static final FullQualifiedName ET_METRIC_FQN = new FullQualifiedName(NAMESPACE, ET_METRIC_NAME);
	public static final String ET_MEASUREMENT_NAME = "Measurement";
	public static final FullQualifiedName ET_MEASUREMENT_FQN = new FullQualifiedName(NAMESPACE, ET_MEASUREMENT_NAME);

	// Entity Set Names
	public static final String ES_PRODUCTS_NAME = "Products";
	public static final String ES_APPLICATIONS_NAME = "Applications";
	public static final String ES_SENSORS_NAME = "Sensors";
	public static final String ES_METRICS_NAME = "Metrics";
	public static final String ES_MEASUREMENTS_NAME = "Measurements";

	@Override
	public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) {
		// this method is called for one of the EntityTypes that are configured
		// in the Schema
		if (ET_PRODUCT_FQN.equals(entityTypeName)) {

			// create EntityType properties
			CsdlProperty id = new CsdlProperty().setName("ID")
					.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
			CsdlProperty name = new CsdlProperty().setName("Name")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty description = new CsdlProperty().setName("Description")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

			// create PropertyRef for Key element
			CsdlPropertyRef propertyRef = new CsdlPropertyRef();
			propertyRef.setName("ID");

			// configure EntityType
			CsdlEntityType entityType = new CsdlEntityType();
			entityType.setName(ET_PRODUCT_NAME);
			entityType.setProperties(Arrays.asList(id, name, description));
			entityType.setKey(Collections.singletonList(propertyRef));

			return entityType;
		} else if (ET_APPLICATION_FQN.equals(entityTypeName)) {
			//System.out.println("get entity type Application");
			// create EntityType properties
			CsdlProperty id = new CsdlProperty().setName("appId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty name = new CsdlProperty().setName("appName")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty description = new CsdlProperty().setName("appDesc")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

			// create PropertyRef for Key element
			CsdlPropertyRef propertyRef = new CsdlPropertyRef();
			propertyRef.setName("appId");

			// configure EntityType
			CsdlEntityType entityType = new CsdlEntityType();
			entityType.setName(ET_APPLICATION_NAME);
			entityType.setProperties(Arrays.asList(id, name, description));
			entityType.setKey(Collections.singletonList(propertyRef));

			return entityType;
		} else if (ET_SENSOR_FQN.equals(entityTypeName)) {
			//System.out.println("get entity type Sensor");
			// create EntityType properties
			CsdlProperty appId = new CsdlProperty().setName("appId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty sensorId = new CsdlProperty().setName("sensorId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty sensorName = new CsdlProperty().setName("sensorName")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty sensorDesc = new CsdlProperty().setName("sensorDesc")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

			// create PropertyRef for Key element
			CsdlPropertyRef propertyRef = new CsdlPropertyRef();
			propertyRef.setName("sensorId");

			// configure EntityType
			CsdlEntityType entityType = new CsdlEntityType();
			entityType.setName(ET_SENSOR_NAME);
			entityType.setProperties(Arrays.asList(appId, sensorId, sensorName, sensorDesc));
			entityType.setKey(Collections.singletonList(propertyRef));

			return entityType;
		} else if (ET_METRIC_FQN.equals(entityTypeName)) {
			//System.out.println("get entity type Metric");
			// create EntityType properties
			CsdlProperty appId = new CsdlProperty().setName("appId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty sensorId = new CsdlProperty().setName("sensorId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty metricId = new CsdlProperty().setName("metricId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty typeOfData = new CsdlProperty().setName("typeOfData")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty mUnit = new CsdlProperty().setName("mUnit")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty value = new CsdlProperty().setName("value")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty timestamp = new CsdlProperty().setName("timestamp")
					.setType(EdmPrimitiveTypeKind.Int64.getFullQualifiedName());
			// create PropertyRef for Key element
			CsdlPropertyRef propertyRef = new CsdlPropertyRef();
			propertyRef.setName("metricId");

			// configure EntityType
			CsdlEntityType entityType = new CsdlEntityType();
			entityType.setName(ET_METRIC_NAME);
			entityType.setProperties(Arrays.asList(appId, sensorId, metricId, typeOfData, mUnit, value, timestamp));
			entityType.setKey(Collections.singletonList(propertyRef));

			return entityType;
		} else if (ET_MEASUREMENT_FQN.equals(entityTypeName)) {
			//System.out.println("get entity type Measurement");
			// create EntityType properties
			CsdlProperty appId = new CsdlProperty().setName("appId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty sensorId = new CsdlProperty().setName("sensorId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty metricId = new CsdlProperty().setName("metricId")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty typeOfData = new CsdlProperty().setName("typeOfData")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty mUnit = new CsdlProperty().setName("mUnit")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty value = new CsdlProperty().setName("value")
					.setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
			CsdlProperty timestamp = new CsdlProperty().setName("timestamp")
					.setType(EdmPrimitiveTypeKind.Int64.getFullQualifiedName());
			// create PropertyRef for Key element
			CsdlPropertyRef propertyRef = new CsdlPropertyRef();
			propertyRef.setName("metricId");

			// configure EntityType
			CsdlEntityType entityType = new CsdlEntityType();
			entityType.setName(ET_MEASUREMENT_NAME);
			entityType.setProperties(Arrays.asList(appId, sensorId, metricId, typeOfData, mUnit, value, timestamp));
			entityType.setKey(Collections.singletonList(propertyRef));

			return entityType;
		}
		return null;

	}

	@Override
	public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName) {
		//System.out.println("Get entity set");
		if (entityContainer.equals(CONTAINER)) {
			if (entitySetName.equals(ES_PRODUCTS_NAME)) {
				CsdlEntitySet entitySet = new CsdlEntitySet();
				entitySet.setName(ES_PRODUCTS_NAME);
				entitySet.setType(ET_PRODUCT_FQN);

				return entitySet;
			} else if (entitySetName.equals(ES_APPLICATIONS_NAME)) {
				CsdlEntitySet entitySet = new CsdlEntitySet();
				entitySet.setName(ES_APPLICATIONS_NAME);
				entitySet.setType(ET_APPLICATION_FQN);

				return entitySet;
			} else if (entitySetName.equals(ES_SENSORS_NAME)) {
				CsdlEntitySet entitySet = new CsdlEntitySet();
				entitySet.setName(ES_SENSORS_NAME);
				entitySet.setType(ET_SENSOR_FQN);

				return entitySet;
			} else if (entitySetName.equals(ES_METRICS_NAME)) {
				CsdlEntitySet entitySet = new CsdlEntitySet();
				entitySet.setName(ES_METRICS_NAME);
				entitySet.setType(ET_METRIC_FQN);

				return entitySet;
			} else if (entitySetName.equals(ES_MEASUREMENTS_NAME)) {
				CsdlEntitySet entitySet = new CsdlEntitySet();
				entitySet.setName(ES_MEASUREMENTS_NAME);
				entitySet.setType(ET_MEASUREMENT_FQN);

				return entitySet;
			}
		}

		return null;

	}

	@Override
	public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {
		// This method is invoked when displaying the service document
		// at e.g. http://localhost:8080/DemoService/DemoService.svc
		if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
			CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
			entityContainerInfo.setContainerName(CONTAINER);
			return entityContainerInfo;
		}

		return null;

	}

	@Override
	public List<CsdlSchema> getSchemas() {
		// create Schema
		CsdlSchema schema = new CsdlSchema();
		schema.setNamespace(NAMESPACE);

		// add EntityTypes
		List<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();
		entityTypes.add(getEntityType(ET_PRODUCT_FQN));
		entityTypes.add(getEntityType(ET_APPLICATION_FQN));
		entityTypes.add(getEntityType(ET_SENSOR_FQN));
		entityTypes.add(getEntityType(ET_METRIC_FQN));
		entityTypes.add(getEntityType(ET_MEASUREMENT_FQN));

		schema.setEntityTypes(entityTypes);

		// add EntityContainer
		schema.setEntityContainer(getEntityContainer());

		// finally
		List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
		schemas.add(schema);

		return schemas;

	}

	@Override
	public CsdlEntityContainer getEntityContainer() {
		// create EntitySets
		List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
		entitySets.add(getEntitySet(CONTAINER, ES_PRODUCTS_NAME));
		entitySets.add(getEntitySet(CONTAINER, ES_APPLICATIONS_NAME));
		entitySets.add(getEntitySet(CONTAINER, ES_SENSORS_NAME));
		entitySets.add(getEntitySet(CONTAINER, ES_METRICS_NAME));
		entitySets.add(getEntitySet(CONTAINER, ES_MEASUREMENTS_NAME));

		// create EntityContainer
		CsdlEntityContainer entityContainer = new CsdlEntityContainer();
		entityContainer.setName(CONTAINER_NAME);
		entityContainer.setEntitySets(entitySets);

		return entityContainer;

	}

}
