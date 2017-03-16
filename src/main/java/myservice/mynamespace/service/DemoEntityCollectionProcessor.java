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

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.IdOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;

import myservice.mynamespace.data.Storage;

public class DemoEntityCollectionProcessor implements EntityCollectionProcessor {

	private OData odata;
	private ServiceMetadata serviceMetadata;
	private Storage storage;

	public DemoEntityCollectionProcessor(Storage storage) {
		this.storage = storage;
	}

	public void init(OData odata, ServiceMetadata serviceMetadata) {
		this.odata = odata;
		this.serviceMetadata = serviceMetadata;
	}

	static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
			ContentType responseFormat) throws ODataApplicationException, SerializerException {
		System.out.println("readEntityCollection");
		// 1st retrieve the requested EntitySet from the uriInfo (representation
		// of the parsed URI)
		List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
		UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
		EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

		FilterOption filterOption = uriInfo.getFilterOption();

		// 2nd: fetch the data from backend for this requested EntitySetName and
		// deliver as EntitySet
		EntityCollection entityCollection = null;

		if (edmEntitySet.getName().equals(DemoEdmProvider.ES_MEASUREMENTS_NAME) && filterOption != null) {
			String uriString = request.getRawQueryPath();
			System.out.println(uriString);
			String[] getFilter = uriString.split("$filter=");
			String[] sepAnds = getFilter[getFilter.length - 1].split("and");
			if (sepAnds.length > 1) {
				String metricId = getId(sepAnds[0]);
				String[] gt = sepAnds[1].split("gt");
				Long timestamp1 = getTimestamp(gt[gt.length - 1]);
				String[] lt = sepAnds[2].split("lt");
				Long timestamp2 = getTimestamp(lt[lt.length - 1]);
				entityCollection = storage.getMeasurements(metricId, timestamp1, timestamp2);
			}
		} else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_SENSORS_NAME) && filterOption != null) {
			String uriString = request.getRawQueryPath();
			String[] getFilter = uriString.split("=");
			String[] keyText = getFilter[1].split("%20eq%20%27");
			keyText[1] = keyText[1].replaceAll("%27", "");
			if (keyText[0].compareTo("appId") != 0) {
				throw new ODataApplicationException("Sensors for requested application doesn't exists",
						HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
			} else {
				entityCollection = storage.getAppSensors(keyText[0], keyText[1]);
			}

		} else if (edmEntitySet.getName().equals(DemoEdmProvider.ES_METRICS_NAME) && filterOption != null) {
			String uriString = request.getRawQueryPath();
			String[] getFilter = uriString.split("=");
			String[] keyText = getFilter[1].split("%20eq%20%27");
			keyText[1] = keyText[1].replaceAll("%27", "");
			if (keyText[0].compareTo("sensorId") != 0) {
				throw new ODataApplicationException("Metrics for requested sensor doesn't exists",
						HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
			} else {
				entityCollection = storage.getSensorMetrics(keyText[0], keyText[1]);
			}
		} else {
			entityCollection = storage.readEntitySetData(edmEntitySet);
			// Apply $filter system query option
			if (filterOption != null) {
				try {
					List<Entity> entityList = entityCollection.getEntities();
					Iterator<Entity> entityIterator = entityList.iterator();
					// Evaluate the expression for each entity
					// If the expression is evaluvisitorResultated to "true",
					// keep
					// the entity
					// otherwise remove it from the entityList
					while (entityIterator.hasNext()) {
						// To evaluate the the expression, create an instance of
						// the
						// Filter Expression Visitor and pass
						// the current entity to the constructor
						Entity currentEntity = entityIterator.next();
						Expression filterExpression = filterOption.getExpression();
						FilterExpressionVisitor expressionVisitor = new FilterExpressionVisitor(currentEntity);

						// Start evaluating the expression
						Object visitorResult = filterExpression.accept(expressionVisitor);

						// The result of the filter expression must be of type
						// Edm.Boolean
						if (visitorResult instanceof Boolean) {
							System.out.println("true");
							if (!Boolean.TRUE.equals(visitorResult)) {
								// The expression evaluated to false (or null),
								// so
								// we have to remove the currentEntity from
								// entityList
								entityIterator.remove();
							}
						} else {
							throw new ODataApplicationException("A filter expression must evaulate to type Edm.Boolean",
									HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
						}
					}

				} catch (ExpressionVisitException e) {
					throw new ODataApplicationException("Exception in filter evaluation",
							HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ENGLISH);
				}
			}
		}

		// 3rd: create a serializer based on the requested format (json)
		ODataSerializer serializer = odata.createSerializer(responseFormat);

		// and serialize the content: transform from the EntitySet object to
		// InputStream
		EdmEntityType edmEntityType = edmEntitySet.getEntityType();
		ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

		final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
		EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id).contextURL(contextUrl)
				.build();
		SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType,
				entityCollection, opts);
		InputStream serializedContent = serializerResult.getContent();

		// 4th: configure the response object: set the body, headers and status
		// code
		response.setContent(serializedContent);
		response.setStatusCode(HttpStatusCode.OK.getStatusCode());
		response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	}

	/**
	 * returns uri id
	 * 
	 * @param uri
	 * @return
	 */
	private String getId(String uri) {
		String[] removedeq = uri.split("=");
		String[] query = removedeq[1].split("%20");
		String id = query[query.length - 1].replace("%27", "");
		System.out.println("MetricId = " + id);
		return id;
	}

	private Long getTimestamp(String uri) {

		String ts = uri.replace("%", "");
		ts = uri.replace("%20", "");
		System.out.println("Timestamp = " + Long.valueOf(ts));
		return Long.valueOf(ts);
	}

}
