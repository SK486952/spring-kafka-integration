package com.kafka.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import com.kafka.constant.ApplicationConstant;

@Service
@Configuration
@EnableKafka
public class SpringKafkaConfigserviceImpl implements SpringKafkaConfigservice{
	private static AtomicInteger msg_received_counter = new AtomicInteger(0);

	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG);
		configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configMap.put(JsonDeserializer.TRUSTED_PACKAGES, "com.netsurfingzone.dto");
		return new DefaultKafkaProducerFactory<String, Object>(configMap);
	}

	@Bean
	public KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	
	public  Properties getConsumerProps(String consumerGroup) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG);
        props.setProperty("group.id", consumerGroup);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "40000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "25000");        
        return props;
	}
	

	public  void startConsumer(String consumerId, String consumerGroup,String TOPIC_NAME) {
	      System.out.printf("starting consumer: %s, group: %s%n", consumerId, consumerGroup);
	      Properties consumerProps = getConsumerProps(consumerGroup);
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
	      consumer.subscribe(Collections.singleton(TOPIC_NAME));
	      while (true) {
	          ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
	          for (ConsumerRecord<String, String> record : records) {
	              msg_received_counter.incrementAndGet();
	              System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s"
	                              + ", offset = %s%n",
	                      consumerId, record.partition(),record.key(), record.value(), record.offset());
	          }

	          consumer.commitSync();
				/*
				 * if(msg_received_counter.get()== totalMsgToSend){ break; }
				 */
	      }
	      
	      
	}
	
	public  Map<String, Object> createTopic(String topic,int partitions,int replicationfactor) {
		CreateTopicsResult result;
  	  Map<String, Object> response = new HashMap<>();
  	  try {
  		
  	 Properties properties = new Properties();
  	    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG);
  	    AdminClient kafkaAdminClient = KafkaAdminClient.create(properties);
  	     result = kafkaAdminClient.createTopics(
  	            Stream.of(topic).map(
  	                    name -> new NewTopic(name, partitions, (short) replicationfactor)
  	            ).collect(Collectors.toList())
  	    );
  	  
		      result.all().get();
		      response.put("response", "Topic created succssfully");
		      
			 
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				response.put("response", e);
			      
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				response.put("response", e);
			}
  	  return response;
	}
	
	public void run(String topicname,int consumerCount) {
		
  	  Map<String, Object> response = new HashMap<>();
  	  try {
  		 String[] consumerGroups = new String[3];
		    for (int i = 0; i < consumerGroups.length; i++) {
		          consumerGroups[i] ="consumer-group-"+i;
		      }   
		      ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
		      for (int i = 0; i < consumerCount; i++) {
		          String consumerId = Integer.toString(i + 1);
		          int finalI = i;
		          executorService.execute(() ->startConsumer(consumerId, consumerGroups[finalI],topicname));
		      }
		      executorService.shutdown();
		      executorService.awaitTermination(10, TimeUnit.MINUTES);
		  
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
	}

	
	
	
	
	
}
