package com.kafka.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.config.SpringKafkaConfigservice;
import com.kafka.constant.ApplicationConstant;
import com.kafka.dto.Student;

@RestController
@RequestMapping("/kafka")
public class Kafkacontroller {


	
	@Autowired
	private SpringKafkaConfigservice service;

	@PostMapping("/createTopic")
    public Map<String, Object> createTopic(@RequestParam String topic,@RequestParam int partitions,@RequestParam int replicationfactor) {
		
		Map<String, Object> response=service.createTopic(topic, partitions, replicationfactor);
    	
		return response;
	
    	  }
	
	@PostMapping("/sendmessages")
	public Map<String, Object> sendMessage(@RequestParam String topicname,@RequestBody Student message) {
	  	 
		Map<String, Object> response = new HashMap<>();
		try {
			service.kafkaTemplate().send(topicname,message);
			response.put(topicname, "json message sent successfully");
		} catch (Exception e) {
			e.printStackTrace();
			response.put(topicname, e);
		}
		return response;
	}
	
	@PostMapping("/subscribetoTopic")
	public  void consumemessages(@RequestParam String topicname,@RequestParam int consumerCount) throws Exception {
        service.run(topicname, consumerCount);
     	}

}
