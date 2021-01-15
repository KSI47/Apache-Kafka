package com.dassault_systemes.schemaregistryclient;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.avro.generic.GenericRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

/**
 * A class that implements static methods to work as Kafka clients with schema
 * validation through Schema Registry and REST Proxy confluent components, this
 * class implements a specific schema parser as a test to send and read data
 * from kafka. it also provides static methods to extract schemas from Schema
 * Registry using id/subject_name-version
 * 
 * @author SKI44
 */
public class Client {
	/**
	 * logger class for methods logging
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientMain.class);

	/**
	 * producer properties for producing clients
	 */
	static private Properties prod_props = new Properties();

	/**
	 * consumer properties for consuming clients
	 */
	static private Properties cons_props = new Properties();

	/**
	 * gets producer properties
	 * 
	 * @return client producer properties
	 */
	public static Properties getProdProps() {
		return prod_props;
	}

	/**
	 * gets consumer properties
	 * 
	 * @return client consumer properties
	 */
	public static Properties getConsProps() {
		return cons_props;
	}

	/**
	 * sets producer properties
	 * 
	 * @param props producer properties
	 */
	public static void setProdProps(Properties props) {
		prod_props = (Properties) props.clone();
	}

	/**
	 * sets consumer properties
	 * 
	 * @param props consumer properties
	 */
	public static void setConsProps(Properties props) {
		cons_props = (Properties) props.clone();
	}

	/**
	 * Retrieves schema from Schema Registry using id registered number
	 * 
	 * @param SchemaRegisteryUrl Schema registry URL with no '/' at the end
	 * @param id                 the schema_id registered by Schema Registry
	 * @return the schema (avro/json)
	 * @throws IOException
	 */
	static public String getSchema(String SchemaRegitryUrl, int id) throws IOException {
		String Schema = null;
		URL restProxyUrl = new URL(SchemaRegitryUrl + "/schemas/ids/" + id); // URL schema resource
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection(); // Start connection
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Content-Type", "application/json");
		int responseCode = connection.getResponseCode(); // get HTTP response code
		String readLine;
		if (responseCode == HttpURLConnection.HTTP_OK) { // Http success
			// buffering input data
			BufferedReader inputBuffer = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			StringBuffer response = new StringBuffer(); // Json response
			while ((readLine = inputBuffer.readLine()) != null) { // read line after line with from input buffer until
																	// the end null
				response.append(readLine);
			}
			try {
				// json parsing to the HTTP request to extract schema
				JSONObject SchemaJson = (JSONObject) new JSONParser().parse(response.toString());
				Schema = (String) SchemaJson.get("schema");
			} catch (ParseException e) { // error json parsing
				e.printStackTrace();
			}
			inputBuffer.close();
			return Schema;
		} else {
			return null;
		}
	}

	/**
	 * Retrieves schema from Schema Registry using subject name and version
	 * 
	 * @param SchemaRegisteryUrl Schema registry URL with no '/' at the end
	 * @param subject            the subject name generated by the Schema Registry
	 *                           to store schemas
	 * @param version            the subject version of the schema
	 * @return the schema avro/json
	 * @throws IOException
	 */
	static public String getSchema(String SchemaRegisteryUrl, String subject, int version) throws IOException {
		String Schema = null;
		URL restProxyUrl = new URL(SchemaRegisteryUrl + "/subjects/" + subject + "/versions/" + version); // URL schema
																											// resource
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection(); // Start connection
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Content-Type", "application/json");
		int responseCode = connection.getResponseCode(); // get HTTP response code
		String readLine;
		if (responseCode == HttpURLConnection.HTTP_OK) { // Http success
			BufferedReader inputBuffer = new BufferedReader(new InputStreamReader(connection.getInputStream())); // buffering
																													// input
																													// data
			StringBuffer response = new StringBuffer(); // Json response
			while ((readLine = inputBuffer.readLine()) != null) {// read line after line from input buffer until the end
																	// null
				response.append(readLine);
			}
			try { // json parsing to the HTTP request to extract schema
				JSONObject SchemaJson = (JSONObject) new JSONParser().parse(response.toString());
				Schema = (String) SchemaJson.get("schema");
			} catch (ParseException e) { // error json parsing
				e.printStackTrace();
			}
			inputBuffer.close();
			return Schema;
		} else {
			return null;
		}
	}

	/**
	 * Produces a message to a topic in Avro format with in a specific schema
	 * [String,String,String] for a specified number of times
	 * 
	 * @param topic       the topic to write on
	 * @param inputs      message to write in format [String,String,String]
	 * @param nbrMessages number of times to produce the input message
	 * @throws IOException
	 */
	static public void produceAvro(String topic, String[] inputs, int nbrMessages) throws IOException {
		// producer in generic records which will be formatted using schema value
		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(prod_props);
		Schema.Parser parser = new Schema.Parser();

		// schema to parse input message with
		String AvroSchemaSample = "{ \"type\": \"record\", \"name\": \"Json\","
				+ "\"namespace\": \"com.dassault_systemes.JsonSchemas\", \"fields\": ["
				+ "{\"name\": \"input1\", \"type\": \"int\"},{ \"name\": \"input2\", \"type\":"
				+ "\"string\"},{ \"name\": \"input3\", \"type\": \"string\"} ] }";

		// generating GenericRecord object formatted in AvroSchemaSample String
		GenericRecord avroValue = new GenericData.Record(parser.parse(AvroSchemaSample));

		if (inputs.length != 3) { // mismatch input size error
			logger.error("mismatch input size : Input data for avro producer doesn't match schema \n"
					+ getSchema(prod_props.get("schema.registry.url").toString().split(",")[0], 86) + "\n"
					+ ", this can be happening because of the currently implemented test that uses a particular schema for test purposes ");
		} else {
			// loading input data according to AvroSchemaSample
			avroValue.put("input1", inputs[0]);
			avroValue.put("input2", inputs[1]);
			avroValue.put("input3", inputs[2]);
		}

		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic,
				"id-" + (System.currentTimeMillis() % 100), avroValue); // Avroschema record constructor
		try {
			// producing
			for (int i = 0; i < nbrMessages; i++) {
				producer.send(record);
			}
		} catch (SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush(); // immediate sending buffered records regardless of linger.ms (optional)
			producer.close();
		}
	}

	/**
	 * Produces a message to a topic in Avro format with in a specific schema
	 * [String,String,String] retrieved from Schema Registry by subject name and
	 * version for a specified number of times
	 * 
	 * @param topic       the topic to write on
	 * @param subject     subject name of a schema
	 * @param version     version of the subject
	 * @param inputs      message to write in format [String,String,String]
	 * @param nbrMessages number of times to produce the input message
	 * @throws IOException
	 */
	static public void produceAvro(String topic, String subject, int version, String[] inputs, int nbrMessages)
			throws IOException {
		// producer in generic records which will be formatted using schema value
		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(prod_props);
		Schema.Parser parser = new Schema.Parser();

		// parse schema retrieved by subject-name and version from the first configured
		// Schema Registry node and create the generic record formatted instance
		GenericRecord avroValue = new GenericData.Record(parser
				.parse(getSchema(prod_props.get("schema.registry.url").toString().split(",")[0], subject, version)));

		if (inputs.length != 3) { // mismatch input size error
			logger.error("mismatch input size :Input data for avro producer doesn't match schema subject" + subject
					+ "/versions/" + version
					+ ", this can be happening because of the currently implemented test that uses a particular schema for test purposes ");
		} else {
			// loading input data according to AvroSchemaSample
			avroValue.put("input1", inputs[0]);
			avroValue.put("input2", inputs[1]);
			avroValue.put("input3", inputs[2]);
		}

		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic,
				"id-" + (System.currentTimeMillis() % 100), avroValue);
		try {
			// producing
			for (int i = 0; i < nbrMessages; i++) {
				producer.send(record);
			}
		} catch (SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush(); // immediate sending buffered records regardless of linger.ms (optional)
			producer.close();
		}
	}

	/**
	 * Produces a message to a topic in Avro format with in a specific schema
	 * [String,String,String] retrieved from Schema Registry by id for a specified
	 * number of times
	 * 
	 * @param topic       the topic to write on
	 * @param id          the schema id in Schema Registry
	 * @param inputs      message to write in format [String,String,String]
	 * @param nbrMessages number of times to produce the input message
	 * @throws IOException
	 */
	static public void produceAvro(String topic, int id, String[] inputs, int nbrMessages) throws IOException {
		// producer in generic records which will be formatted using schema value
		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(prod_props);
		Schema.Parser parser = new Schema.Parser();

		// parse schema retrieved by id from the first configured Schema Registry node
		// and create the generic record formatted instance
		GenericRecord avroValue = new GenericData.Record(
				parser.parse(getSchema(prod_props.get("schema.registry.url").toString().split(",")[0], id)));

		if (inputs.length != 3) { // mismatch input size error
			logger.error("Input data for avro producer doesn't match schema id" + id
					+ ", this can be happening because of the currently implemented test that uses a particular schema for test purposes ");
		} else { // loading input data according to AvroSchemaSample
			avroValue.put("input1", inputs[0]);
			avroValue.put("input2", inputs[1]);
			avroValue.put("input3", inputs[2]);

			ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, //
					"id-" + (System.currentTimeMillis() % 100), avroValue);
			try {
				// producing
				for (int i = 0; i < nbrMessages; i++) {
					producer.send(record);
				}
			} catch (SerializationException e) {
				e.printStackTrace();
			} finally {
				producer.flush(); // immediate sending buffered records regardless of linger.ms (optional)
				producer.close();
			}
		}

	}

	/**
	 * Consumes messages in Avro and displays them from a topic with a configurable
	 * poll timeout. it will keep consuming messages until poll returns empty
	 * records.
	 * 
	 * @param topic       the topic to read from
	 * @param pollTimeout the timeout that poll method waits for when there's no
	 *                    records received yet before stop polling
	 */
	static public void consumeAvro(String topic, long pollTimeout) {
		// consume generic records
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(cons_props);
		consumer.subscribe(Arrays.asList(topic));
		try {
			ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(pollTimeout));
			// polling loop
			while (!records.isEmpty()) {
				for (ConsumerRecord<String, GenericRecord> record : records) {

					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(),
							record.value());
				}
				// Asynchronous committing offsets
				consumer.commitAsync();
				records = consumer.poll(Duration.ofMillis(pollTimeout));
			}
		} finally {
			consumer.close();
		}

	}

	/**
	 * Produces a message to a topic in JsonSchema format with in a specific schema
	 * [String,String,String] defined as fields in a type class EvenSchema for a
	 * specified number of times
	 * 
	 * @param topic       the topic to write on
	 * @param inputs      message to write in format [String,String,String]
	 * @param nbrMessages number of times to produce the input message
	 */
	static public void produceJson(String topic, String[] inputs, int nbrMessages) {
		// Producer with class schema
		KafkaProducer<String, EventSchemaJson> producer = new KafkaProducer<String, EventSchemaJson>(prod_props);

		if (inputs.length != 3) { // mismatch input size error
			logger.error("Input data for Json producer doesn't match schema in used class"
					+ ", this can be happening because of the currently implemented test that uses a particular schema for test purposes ");
		}

		else {
			// record constructor with arbitrary key id < 100
			ProducerRecord<String, EventSchemaJson> record = new ProducerRecord<String, EventSchemaJson>(topic,
					"id-" + (System.currentTimeMillis() % 100), new EventSchemaJson(inputs[0], inputs[1], inputs[2]));
			try {
				// Producing
				for (int i = 0; i < nbrMessages; i++) {
					producer.send(record);
				}
			} catch (SerializationException e) {
				e.printStackTrace();
			} finally {
				producer.flush(); // immediate sending buffered records regardless of linger.ms (optional)
				producer.close();
			}
		}

	}

	/**
	 * Consumes messages in JsonSchema and displays them from a topic with a
	 * configurable poll timeout. it will keep consuming messages until poll return
	 * empty records.
	 * 
	 * @param topic       the topic to read from
	 * @param pollTimeout the timeout that poll method waits for when there's no
	 *                    records received yet before stop polling
	 * @throws JsonProcessingException
	 */
	static public void consumeJson(String topic, long pollTimeout) throws JsonProcessingException {
		// consume EventSChema objects
		final Consumer<String, EventSchemaJson> consumer = new KafkaConsumer<String, EventSchemaJson>(cons_props);

		consumer.subscribe(Arrays.asList(topic));

		ObjectMapper mapper = new ObjectMapper(); // Mapper object to create json format record from EventSchema objects

		try {
			ConsumerRecords<String, EventSchemaJson> records = consumer.poll(Duration.ofMillis(pollTimeout));
			while (!records.isEmpty()) { // Polling loop
				for (ConsumerRecord<String, EventSchemaJson> record : records) {

					// formatting objects fields into json 
					String recordValueJson = mapper.writeValueAsString(record.value());

					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(),
							recordValueJson);
					consumer.commitAsync(); // Asynchronous committing offsets
				}
				records = consumer.poll(Duration.ofMillis(pollTimeout));
			}
		} finally {
			consumer.close();
		}

	}

	/**
	 * Produces JsonSchema messages through REST Proxy with schema
	 * [String,String,String]
	 * 
	 * @param urlRestProxy Rest Proxy url (preferred primary node)
	 * @param topic        topic the topic to write on
	 * @param inputs       message to write in format [String,String,String]
	 * @throws IOException
	 */
	public static void produceJsonSchemaREST(String urlRestProxy, String topic, String[] inputs) throws IOException {
		// schema value and insert data into schema
		String jsonInputString = "{ \"value_schema\": \"{\\\"type\\\":\\\"object\\\","
				+ "\\\"properties\\\":{\\\"input1\\\":{\\\"type\\\":\\\"string\\\"},\\\"input2\\\":{\\\"type\\\":\\\"string\\\"},\\\"input3\\\":{\\\"type\\\":\\\"string\\\"}}}\", "
				+ "\"records\": [{ \"value\": {\"input1\": \"" + inputs[0] + "]\",\"input2\": \"" + inputs[1]
				+ "\",\"input3\": \"" + inputs[2] + "\"}}]}";

		// send http post request to Rest Proxy with jsonschema embedded format
		System.out.println(HttpRestProxyRequest.HttpPOSTRequest(urlRestProxy + "/topics/" + topic,
				"application/vnd.kafka.jsonschema.v2+json", "application/vnd.kafka.v2+json", jsonInputString));
	}

	/**
	 * Consumes JsonSchema messages through REST Proxy and deletes consumer instance
	 * before exiting
	 * 
	 * @param URLRestProxy     Rest Proxy url (preffered primary node)
	 * @param topic            the topic to read from
	 * @param consumerName     Consumer Name that will be used by Kafka to
	 *                         distinguich consumers.
	 * @param consumerInstance Consumer instance that will be used by Rest Proxy as
	 *                         consumer instance
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void consumeJsonSchemaREST(String URLRestProxy, String topic, String consumerName,
			String consumerInstance) throws IOException, ParseException {
		// consumer configs : instance name, serializing format and auto offset strategy
		String consumerConfig = "{\"name\": \"" + consumerInstance
				+ "\", \"format\": \"jsonschema\", \"auto.offset.reset\": \"earliest\"}";

		// send POST request to Rest Proxy to create consumer instance in jsonschema
		// format and get back response
		String ConsumerInstanceCreationResponse = HttpRestProxyRequest.HttpPOSTRequest(
				URLRestProxy + "/consumers/" + consumerName, "application/vnd.kafka.jsonschema.v2+json",
				"application/vnd.kafka.v2+json", consumerConfig);

		// parse response and cast it from String to JSON
		JSONObject ConsumerInstanceCreationResponseJSON = (JSONObject) new JSONParser()
				.parse(ConsumerInstanceCreationResponse);

		// extract consumer instance URI necessary for subscription and consuming
		URL ConsumerInstanceUrl = new URL((String) ConsumerInstanceCreationResponseJSON.get("base_uri"));

		// subscribe to topic with the jsonschema serializers
		HttpRestProxyRequest.HttpPOSTRequest(ConsumerInstanceUrl.toString() + "/subscription",
				"application/vnd.kafka.jsonschema.v2+json", "application/vnd.kafka.v2+json",
				"{\"topics\":[\"" + topic + "\"]}");

		// Send GET request to Rest Proxy to consume messages and embed all the records
		// and their metadata (array of records and their metadata) in the HTTP GET
		// response in one string variable to be parsed later
		String RecordsAndMetadata = HttpRestProxyRequest.HttpGETRequest(ConsumerInstanceUrl.toString() + "/records",
				"application/vnd.kafka.jsonschema.v2+json");

		// parse Records and metadata array
		// to be casted into object
		// and then JSONArray object
		Object obj = JSONValue.parse(RecordsAndMetadata);
		JSONArray JSONArrayRecords = (JSONArray) obj;
		for (Object record : JSONArrayRecords) { // go through JSONArray of records and metadata in a loop and extract
													// every single JSONObject
			JSONObject JSONRecord = (JSONObject) new JSONParser().parse(record.toString());

			// display only offset, key and values
			// from each JSONObject in records and
			// metadata
			System.out.printf("offset = %s, key = %s, value =%s \n", JSONRecord.get("offset").toString(),
					JSONRecord.get("key"), JSONRecord.get("value").toString());

		}
		// delete consumer instance after finishing so we don't have conflict issues
		// when we launch back consumer with the same name in a short amount of time
		HttpRestProxyRequest.HttpDELETERequest(ConsumerInstanceUrl.toString(), "application/vnd.kafka.v2+json");

	}

	/**
	 * Produces Avro messages through REST Proxy with schema [String,String,String]
	 * 
	 * @param urlRestProxy Rest Proxy url (preffered primary node)
	 * @param topic        topic the topic to write on
	 * @param inputs       message to write in format [String,String,String]
	 * @throws IOException
	 */
	public static void produceAvroREST(String urlRestProxy, String topic, String[] inputs) throws IOException {
		// schema value and insert data into schema
		String avroInputString = "{\"value_schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"AvroRestSchema\\\","
				+ "\\\"fields\\\":[{\\\"name\\\":\\\"input1\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"input2\\\",\\\"type\\\":\\\"string\\\"},"
				+ "{\\\"name\\\":\\\"input3\\\",\\\"type\\\":\\\"string\\\"}]}\", "
				+ "\"records\": [{\"value\": {\"input1\": \"" + inputs[0] + "\",\"input2\": \"" + inputs[1]
				+ "\",\"input3\": \"" + inputs[2] + "\"}}]}";

		// send http post request to Rest Proxy with jsonschema embedded format
		System.out.println(HttpRestProxyRequest.HttpPOSTRequest(urlRestProxy + "/topics/" + topic,
				"application/vnd.kafka.avro.v2+json", "application/vnd.kafka.v2+json", avroInputString));
	}

	/**
	 * Consumes Avro messages through REST Proxy and deletes consumer instance
	 * before exiting
	 * 
	 * @param URLRestProxy     Rest Proxy url (preferred primary node)
	 * @param topic            the topic to read from
	 * @param consumerName     Consumer Name that will be used by Kafka to
	 *                         Distinguish consumers.
	 * @param consumerInstance Consumer instance that will be used by Rest Proxy as
	 *                         consumer instance
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void consumeAvroREST(String URLRestProxy, String topic, String consumerName, String consumerInstance)
			throws IOException, ParseException {
		// consumer configs : instance name, serializing format and auto offset strategy
		String consumerConfig = "{\"name\": \"" + consumerInstance
				+ "\", \"format\": \"avro\", \"auto.offset.reset\": \"earliest\"}";

		// send POST request to Rest Proxy to create consumer instance in jsonschema
		// format and get back response
		String ConsumerInstanceCreationResponse = HttpRestProxyRequest.HttpPOSTRequest(
				URLRestProxy + "/consumers/" + consumerName, "application/vnd.kafka.v2+json", consumerConfig);

		// Parse response and cast it from String to JSON
		JSONObject ConsumerInstanceCreationResponseJSON = (JSONObject) new JSONParser()
				.parse(ConsumerInstanceCreationResponse);

		// extract consumer instance URI necessary for subscription and consuming
		URL ConsumerInstanceUrl = new URL((String) ConsumerInstanceCreationResponseJSON.get("base_uri"));

		// subscribe to topic with the jsonschema serializers
		HttpRestProxyRequest.HttpPOSTRequest(ConsumerInstanceUrl.toString() + "/subscription",
				"application/vnd.kafka.v2+json", "{\"topics\":[\"" + topic + "\"]}");

		// Send GET request to Rest Proxy to consume messages and embed all the records
		// and their metadata (array of records and their metadata) in the HTTP GET
		// response in one string variable to be parsed later
		String RecordsAndMetadata = HttpRestProxyRequest.HttpGETRequest(ConsumerInstanceUrl.toString() + "/records",
				"application/vnd.kafka.avro.v2+json");
		// parse Records and metadata array to be casted into object and then JSONArray
		// object
		Object obj = JSONValue.parse(RecordsAndMetadata);
		JSONArray JSONArrayRecords = (JSONArray) obj;
		for (Object record : JSONArrayRecords) { // go through JSONArray of records and metadata in a loop and extract
													// every single JSONObject
			JSONObject JSONRecord = (JSONObject) new JSONParser().parse(record.toString());

			// display only offset, key and values from each JSONObject in records and
			// metadata
			System.out.printf("offset = %s, key = %s, value =%s \n", JSONRecord.get("offset").toString(),
					JSONRecord.get("key"), JSONRecord.get("value").toString());
		}
		
		// delete consumer instance after finishing so we don't have conflict issues
		// when we launch back consumer with the same name in a short amount of time
		HttpRestProxyRequest.HttpDELETERequest(ConsumerInstanceUrl.toString(), "application/vnd.kafka.v2+json");

	}
}