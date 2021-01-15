package com.dassault_systemes.schemaregistryclient;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.json.simple.parser.ParseException;

public class ClientMain {

	public Properties getPropsFromFile(String path) throws IOException {
		Properties props = new Properties();
		InputStream is = getClass().getClassLoader().getResourceAsStream(path);
		if (is != null) {
			props.load(is);
			return props;
		} else {
			throw new FileNotFoundException("propertie file path " + path + "couldn't succesfully be read");
		}
	}
	
	public static void testingFlag(String TestName) {
		System.out.println("\n"+TestName);
		for (int i = 0; i < 10; i++) {
			for (int k = 0; k < 10 - i; k++) {
				System.out.printf("#");
			}
			System.out.printf("\n");

		}
	}
	
	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		Properties prod_props = new Properties(), cons_props = new Properties();

		// Data Input according to a template valid schema
		String[] input = new String[] { "value1", "value2", "value3" };

		// Avro

		testingFlag("AvroTest");
		
		// Start executing in 3 seconds
		Thread.sleep(3000);
		prod_props = new ClientMain().getPropsFromFile("avro-producer.properties");
		cons_props = new ClientMain().getPropsFromFile("avro-consumer.properties");
		Client.setProdProps(prod_props);
		Client.setConsProps(cons_props);
		Client.produceAvro("TopicAvroNew", "TopicAvro-value", 1, input, 10);
		Client.consumeAvro("TopicAvroNew", 10000);

		// JsonSchema

		testingFlag("JSONSchemaTest");
		
		// Start executing in 3 seconds
		Thread.sleep(3000);
		prod_props = new ClientMain().getPropsFromFile("jsonschema-producer.properties");
		cons_props = new ClientMain().getPropsFromFile("jsonschema-consumer.properties");
		Client.setProdProps(prod_props);
		Client.setConsProps(cons_props);
		Client.produceJson("JsonSchemaTopicNew", input, 10);
		Client.consumeJson("JsonSchemaTopicNew", 10000);

		// REST-Proxy

		testingFlag("RESTProxyJSONSchemaTest");
		
		// Start executing in 3 seconds
		Thread.sleep(3000);
		Client.produceJsonSchemaREST(
				new ClientMain().getPropsFromFile("additional-configs.properties").getProperty("schema.registery.url"),
				"newTestTopic", input);
		Client.consumeJsonSchemaREST(
				new ClientMain().getPropsFromFile("additional-configs.properties").getProperty("schema.registery.url"),
				"newTestTopic", "ConsumerRandName" + RandomStringUtils.random(12, true, true), "consumerInstance0");

		testingFlag("RESTProxyAvroTest");
		
		// Start executing in 3 seconds
		Thread.sleep(3000);
		Client.produceAvroREST(
				new ClientMain().getPropsFromFile("additional-configs.properties").getProperty("schema.registery.url"),
				"avrotest4", input);
		Client.consumeAvroREST(
				new ClientMain().getPropsFromFile("additional-configs.properties").getProperty("schema.registery.url"),
				"avrotest4", "ConsumerRandName" + RandomStringUtils.random(12, true, true), "consumerInstance0");
	}

}
