package com.kafka.config;

import java.util.Map;
import java.util.Properties;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.RequestParam;

public interface  SpringKafkaConfigservice {

	
	ProducerFactory<String, Object> producerFactory();
	
	KafkaTemplate<String, Object> kafkaTemplate();
	
	Properties getConsumerProps(String consumerGroup);
		
	Map<String, Object> createTopic(String topic,int partitions,int replicationfactor);
	
	void run(String topicname,int consumerCount);
}
