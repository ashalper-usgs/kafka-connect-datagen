/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafka.connect.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;

public class WaterServicesTask extends SourceTask {

	static final Logger log = LoggerFactory.getLogger(WaterServicesTask.class);

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
	private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();

	private WaterServicesConnectorConfig config;
	private String url;
	private int responseCode = -1;
	private String topic;
	private String schemaFilename;
	private String schemaKeyField;
	private Entity entity;
	private Generator generator;
	private org.apache.avro.Schema avroSchema;
	private org.apache.kafka.connect.data.Schema ksqlSchema;
	private AvroData avroData;
	
	protected enum Entity {
		// TODO: SITE key is really (agency_cd,site_no)
		USERS("users_schema.avro", "userid"), PAGEVIEWS("pageviews_schema.avro", "viewtime"),
		SITE("site.avro", "site_no");

		private final String schemaFilename;
		private final String keyName;

		Entity(String schemaFilename, String keyName) {
			this.schemaFilename = schemaFilename;
			this.keyName = keyName;
		}

		public String getSchemaFilename() {
			return schemaFilename;
		}

		public String getSchemaKeyField() {
			return keyName;
		}
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		config = new WaterServicesConnectorConfig(props);
		url = config.getURL();
		topic = config.getKafkaTopic();
		schemaFilename = config.getSchemaFilename();
		schemaKeyField = config.getSchemaKeyfield();

		String entityName = config.getEntity();
		if (entityName != "") {
			try {
				entity = Entity.valueOf(entityName.toUpperCase());
				if (entity != null) {
					schemaFilename = entity.getSchemaFilename();
					schemaKeyField = entity.getSchemaKeyField();
				}
			} catch (IllegalArgumentException e) {
				log.warn("Entity '{}' not found: ", entityName, e);
			}
		}

		try {
			generator = new Generator(getClass().getClassLoader().getResourceAsStream(schemaFilename), new Random());
		} catch (IOException e) {
			throw new ConnectException("Unable to read the '" + schemaFilename + "' schema file", e);
		}

		avroSchema = generator.schema();
		avroData = new AvroData(1);
		ksqlSchema = avroData.toConnectSchema(avroSchema);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		if (responseCode != HttpURLConnection.HTTP_OK)
			return null;

		InputStream stream;
		HttpURLConnection connection;
		try {
			URL urlObject = new URL(url);
			connection = (HttpURLConnection) urlObject.openConnection();
			connection.setRequestMethod("GET");
			stream = connection.getInputStream();
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
			return null;
		} catch (IOException e) {
			log.error(e.getMessage());
			return null;
		}
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		final CSVParser parser = new CSVParserBuilder().withSeparator('\t').withIgnoreQuotations(true).build();
		final CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build();

		final List<SourceRecord> records = new ArrayList<>();
		
		try {
			String[] field;
			while ((field = csvReader.readNext()) != null) {
				final GenericRecord message = new Record(avroSchema);
				final List<Object> genericRowValues = new ArrayList<>();
				
				// TODO: seems like we can put schema code outside while loop
				for (org.apache.avro.Schema.Field schemaField : avroSchema.getFields()) {
					final Object value = message.get(schemaField.name());
					if (value instanceof Record) {
						final Record record = (Record) value;
						final Object ksqlValue = avroData.toConnectData(record.getSchema(), record).value();
						Object optionValue = getOptionalValue(ksqlSchema.field(schemaField.name()).schema(), ksqlValue);
						genericRowValues.add(optionValue);
					} else {
						genericRowValues.add(value);
					}
				}

				// Key
				String keyString = "";
				if (!schemaKeyField.isEmpty()) {
					SchemaAndValue schemaAndValue = avroData
							.toConnectData(message.getSchema().getField(schemaKeyField).schema(), message.get(schemaKeyField));
					keyString = schemaAndValue.value().toString();
				}

				// Value
				// TODO: we can parse these column names from RDB header row
				message.put("agency_cd", field[0]);
				message.put("site_no", field[1]);
				message.put("station_nm", field[2]);
				message.put("site_tp_cd", field[3]);
				message.put("dec_lat_va", Double.parseDouble(field[4]));
				message.put("dec_long_va", Double.parseDouble(field[5]));
				message.put("coord_acy_cd", field[6]);
				message.put("dec_coord_datum_cd", field[7]);
				message.put("alt_va", field[8]);
				message.put("alt_acy_va", ".1");
				message.put("alt_datum_cd", field[9]);
				message.put("huc_cd", field[10]);
				final Object messageValue = avroData.toConnectData(avroSchema, message).value();

				SourceRecord record = new SourceRecord(SOURCE_PARTITION, SOURCE_OFFSET, topic, KEY_SCHEMA, keyString,
						avroData.toConnectSchema(avroSchema), messageValue);
				records.add(record);
			}
			responseCode = connection.getResponseCode();
		} catch (IOException e) {
			log.error(e.getMessage());
			return null;
		}
		return records;
	}

	@Override
	public void stop() {
	}

	private org.apache.kafka.connect.data.Schema getOptionalSchema(final org.apache.kafka.connect.data.Schema schema) {
		switch (schema.type()) {
		case BOOLEAN:
			return org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
		case INT32:
			return org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
		case INT64:
			return org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
		case FLOAT64:
			return org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
		case STRING:
			return org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
		case ARRAY:
			return SchemaBuilder.array(getOptionalSchema(schema.valueSchema())).optional().build();
		case MAP:
			return SchemaBuilder.map(getOptionalSchema(schema.keySchema()), getOptionalSchema(schema.valueSchema()))
					.optional().build();
		case STRUCT:
			final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
			for (Field field : schema.fields()) {
				schemaBuilder.field(field.name(), getOptionalSchema(field.schema()));
			}
			return schemaBuilder.optional().build();
		default:
			throw new ConnectException("Unsupported type: " + schema);
		}
	}

	private Object getOptionalValue(final org.apache.kafka.connect.data.Schema schema, final Object value) {
		switch (schema.type()) {
		case BOOLEAN:
		case INT32:
		case INT64:
		case FLOAT64:
		case STRING:
			return value;
		case ARRAY:
			final List<?> list = (List<?>) value;
			return list.stream().map(listItem -> getOptionalValue(schema.valueSchema(), listItem))
					.collect(Collectors.toList());
		case MAP:
			final Map<?, ?> map = (Map<?, ?>) value;
			return map.entrySet().stream().collect(Collectors.toMap(k -> getOptionalValue(schema.keySchema(), k),
					v -> getOptionalValue(schema.valueSchema(), v)));
		case STRUCT:
			final Struct struct = (Struct) value;
			final Struct optionalStruct = new Struct(getOptionalSchema(schema));
			for (Field field : schema.fields()) {
				optionalStruct.put(field.name(), getOptionalValue(field.schema(), struct.get(field.name())));
			}
			return optionalStruct;
		default:
			throw new ConnectException("Invalid value schema: " + schema + ", value = " + value);
		}
	}

} // WaterServicesTask
