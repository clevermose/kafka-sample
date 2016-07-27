package com.kafka.highlevelapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer {

	private static final String TOPIC = "KAFKA_1";
	private static final String GROUP_ID = "haiswang";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "10.249.73.142:2181,10.249.73.143:2181,10.249.73.144:2181");
		props.put("group.id", GROUP_ID);
		
		ConsumerConfig cConfig = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(cConfig);
		
		//topic和消费的线程的映射关系,最好是小于等于Partition的数量,大于Partition的数量没有意义,会空闲着
		Map<String, Integer> topicThreadCount = new HashMap<String, Integer>();
		topicThreadCount.put(TOPIC, 2);
		
		//Topic和stream的映射
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessages = consumer.createMessageStreams(topicThreadCount);
		List<KafkaStream<byte[], byte[]>> messages = topicMessages.get(TOPIC);
		//这边的大小就是topicThreadCount中设定的该topic的消费线程的大小
		int kafkaStreamListSize = messages.size();
		System.out.println("kafka stream list size : " + kafkaStreamListSize);
		ExecutorService es = Executors.newFixedThreadPool(kafkaStreamListSize);
		
		for (KafkaStream<byte[], byte[]> kafkaStream : messages) {
			es.submit(new ReadThread(kafkaStream));
		}
	}
}

class ReadThread implements Runnable {
	
	private KafkaStream<byte[], byte[]> kafkaStream;
	
	public ReadThread(KafkaStream<byte[], byte[]> kafkaStream) {
		this.kafkaStream = kafkaStream;
	}
	
	public void run() {
		ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStream.iterator();
		while(consumerIterator.hasNext()) {
			String msg = new String(consumerIterator.next().message());
			System.out.println("message : " + msg);
		}
	}
}
