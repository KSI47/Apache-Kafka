package com.dassault_systemes.kafkaeo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class EOPV implements Runnable{
	private Properties props;
	public Integer threadID;

	public void setProps(Properties props) {
		this.props = props;
	}
	
	public Properties getProps() {
		return this.props;
	}
	
	public void run() {
		this.write(1000,"TopicInput");
	}

	public void write(int nbrMsg, String TopicInput) {
		String json = null;
		try {
			json = new String(Files.readAllBytes(Paths.get("C:\\Users\\SKI44\\Downloads\\event_monitoring.json")));
		} catch (IOException e1) {
			json = "File was not successfully read because of exception "+e1;
		}
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		if (props.get("transactional.id") == null) {
			
		} else {
			producer.initTransactions();
			try {
				producer.beginTransaction();	
				for ( Integer i = 0; i<nbrMsg ; i++) {
					producer.send(new ProducerRecord<String, String>(TopicInput, "key-1",json+i.toString()));
				}
				producer.commitTransaction();
			} catch (ProducerFencedException e) {
				producer.close();
			} catch (KafkaException e) {
				producer.abortTransaction();
			}
			finally {
				producer.close();
			}
		}

	}

}
