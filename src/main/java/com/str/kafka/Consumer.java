package com.str.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

public class Consumer {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Properties props = new Properties();
	     props.put("bootstrap.servers", "127.0.0.1:6667");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "false");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("test.in"));
	     Producer producer = new Producer();
	     Gson gson = new Gson();
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records) {
	        	 System.out.println("processing: " + record);
	        	 
	        	 Map<String, String> data = gson.fromJson(record.value(), Map.class);
	        	 data.put("result", StringUtils.join(data.values(), " "));
	        	 
	        	 String replyTopic = new String(record.headers().lastHeader("kafka_replyTopic").value());
	        	 ProducerRecord<String, String> pRecord = new ProducerRecord<>(replyTopic, gson.toJson(data));
	        	 
	        	 pRecord.headers().add(record.headers().lastHeader("kafka_correlationId"));
	        	 producer.send(pRecord);
	        	 
	        	 consumer.commitSync();
	         }
	     }
	}
}
