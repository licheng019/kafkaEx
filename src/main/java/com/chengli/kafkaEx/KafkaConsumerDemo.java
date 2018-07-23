package com.chengli.kafkaEx;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
public class KafkaConsumerDemo {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		
		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.commit.interval.ms", "1000");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList("first_topic"));
		while(true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
			for (ConsumerRecord record: consumerRecords) {
				System.out.println(record.value());
				System.out.println(record.offset());
				System.out.println(record.key());
				System.out.println(record.partition());
				System.out.println("~~~~~~~~~~~~~`");
			}
 		}
	}
}
