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
		
		//topic�����ѵ��̵߳�ӳ���ϵ,�����С�ڵ���Partition������,����Partition������û������,�������
		Map<String, Integer> topicThreadCount = new HashMap<String, Integer>();
		topicThreadCount.put(TOPIC, 2);
		
		//Topic��stream��ӳ��
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessages = consumer.createMessageStreams(topicThreadCount);
		List<KafkaStream<byte[], byte[]>> messages = topicMessages.get(TOPIC);
		//��ߵĴ�С����topicThreadCount���趨�ĸ�topic�������̵߳Ĵ�С
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
