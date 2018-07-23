package com.chengli.kafkaEx;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		properties.setProperty("acks", "1");
		properties.setProperty("retries", "3");
		Producer<String, String> producer = new KafkaProducer<>(properties);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "3", "messageTest");
		
		producer.send(producerRecord);
		producer.flush();
		producer.close();
	}
}
