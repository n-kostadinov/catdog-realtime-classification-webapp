package com.niko.catdog;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class MessageProducer {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String BROKER_ADDRESS = "localhost:9092";
	private static final String KAFKA_IMAGE_TOPIC = "catdogimage";
	
	private Producer<String, String> kafkaProducer;
	
	@PostConstruct
	private void initialize() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", BROKER_ADDRESS);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducer = new KafkaProducer<>(properties);
	}	
	
	public void publish(CatDogKafkaDTO catDogEvent) {
		
		logger.info("Publish image with url " + catDogEvent.getUrl());
		String jsonValue;
		try {
			jsonValue = new ObjectMapper().writeValueAsString(catDogEvent);
		} catch (JsonProcessingException ex) {
			throw new IllegalStateException(ex);
		}
		this.kafkaProducer.send(new ProducerRecord<String, String>(KAFKA_IMAGE_TOPIC, jsonValue));
	}
	
	@PreDestroy
	public void close() {
		this.kafkaProducer.close();
	}
	
}
