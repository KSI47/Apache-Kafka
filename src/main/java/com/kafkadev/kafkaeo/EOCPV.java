package com.dassault_systemes.kafkaeo;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

public class EOCPV implements Runnable {
	private Properties ConsProps;
	private Properties ProdProps;

	public void setConsProps(Properties props) {
		this.ConsProps = props;
	}

	public void setProdProps(Properties props) {
		this.ProdProps = props;
	}
	
	public Properties getConsProps() {
		return this.ConsProps;
	}
	
	public Properties getProdProps() {
		return this.ProdProps;
	}

	public void run() {
		this.ReadWrite("TopicInput", "TopicOutput");

	}

	public void ReadWrite(String TopicInput, String TopicOutput) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(ConsProps);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ProdProps);
		String output;
		consumer.subscribe(Arrays.asList(TopicInput));
		// Begin Transaction
		producer.initTransactions();
		try {
			producer.beginTransaction();
			ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(10000));
			while (!messages.isEmpty()) {
				Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
				for (ConsumerRecord<String, String> record : messages) {
					output = record.value().toString()+"-processed";
					producer.send(new ProducerRecord<String, String>(TopicOutput, output));
					offsets.put(new TopicPartition(TopicInput, record.partition()),
							new OffsetAndMetadata(record.offset() + 1, null));
				}
				producer.sendOffsetsToTransaction(offsets, ConsProps.getProperty("group.id"));
				messages = consumer.poll(Duration.ofMillis(10000));
			}

			producer.commitTransaction();
		} catch (ProducerFencedException e) {
		
		} catch (KafkaException e) {
			producer.abortTransaction();
		}
		finally {
			producer.close();
			consumer.close();
		}
	}
}
