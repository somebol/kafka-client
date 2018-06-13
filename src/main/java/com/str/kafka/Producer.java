package com.str.kafka;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer {
	
	private final KafkaProducer<String, String> kProducer;
	
	public Producer() {
		Properties props = new Properties();
		 props.put("bootstrap.servers", "127.0.0.1:6667");
		 props.put("acks", "all");
		 props.put("retries", 0);
//		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		 this.kProducer = new KafkaProducer<>(props);
	}
	
	public void produce(String message) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test.in", UUID.randomUUID().toString(), message);
		System.out.println("sending message: " + message);
		try {
			RecordMetadata recordMetadata = kProducer.send(record).get();
			System.out.println("message sent, offset: " + recordMetadata.offset());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public void send(ProducerRecord<String, String> record) {
		System.out.println("sending message: " + record);
		try {
			RecordMetadata recordMetadata = kProducer.send(record).get();
			System.out.println("message sent, offset: " + recordMetadata.offset());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Producer p = new Producer();
		IntStream.range(0, 5)
				.boxed()
				.forEach(i -> p.produce("msg " + i));
	}
}
