package com.kafka.lowerlevelapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerConnectorSample {
	
	private static final String GROUP_ID = "wanghaisheng";
	private static final String topic = "KAFKA_1";
	
	private ConsumerConfig consumerConfig = null;
	private ConsumerConnector connector = null;
	
	public ConsumerConnectorSample() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "10.249.73.142:2181,10.249.73.143:2181,10.249.73.144:2181");
		props.put("group.id", GROUP_ID);
		//offset�����Զ��ύ,�ɳ����ֶ��ύ
		props.put("auto.commit.enable", "false");
		//ÿ��ZK�ϻ�û�и��������offset��Ϣʱ,offsetȡֵ,���ZK���и��������offsetȡֵ,��ô�����þ���Ч
		//smallest-��ͷ��ʼ����
		//largest-������offset��ʼ����
		props.put("auto.offset.reset", "smallest");
		
		consumerConfig = new ConsumerConfig(props);
		connector = Consumer.createJavaConsumerConnector(consumerConfig);
	}
	
	public void run(String topic, int topicThreadCount) {
		Map<String, Integer> topicWithThreadCount = new HashMap<String, Integer>();
		topicWithThreadCount.put(topic, topicThreadCount);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicWithStreams = connector.createMessageStreams(topicWithThreadCount);
		List<KafkaStream<byte[], byte[]>> topicStreams = topicWithStreams.get(topic);
		
		ExecutorService es = Executors.newFixedThreadPool(topicThreadCount);
		int threadNum = 0;
		for (KafkaStream<byte[], byte[]> kafkaStream : topicStreams) {
			es.submit(new PrintThread("Thread-" + (++threadNum),kafkaStream,connector));
		}
	}
	
	public static void main(String[] args) {
		int topicThreadCount = 2;
		ConsumerConnectorSample consumerConnectorSample = new ConsumerConnectorSample();
		consumerConnectorSample.run(topic, topicThreadCount);
	}
}

class PrintThread extends Thread {
	
	private KafkaStream<byte[], byte[]> stream;
	private ConsumerConnector connector;
	
	public PrintThread(String threadName, KafkaStream<byte[], byte[]> stream, ConsumerConnector connector) {
		this.setName(threadName);
		this.stream = stream;
		this.connector = connector;
	}
	
	public void run() {
		ConsumerIterator<byte[], byte[]> streamIter = stream.iterator();
		while(streamIter.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = streamIter.next();
			System.out.println(this.getName() + " - Partition : " + messageAndMetadata.partition());
			System.out.println(this.getName() + " - Offset : " + messageAndMetadata.offset());
			System.out.println(this.getName() + " - Message : " + new String(messageAndMetadata.message()));
			//������auto.commit.enableΪfalse��ʱ��,����Լ��ֶ��ύoffset
			//���Լ����ύʱ,��ôÿ������ʱ,��zk�л�ȡoffset��Ϣ,���Խ��ж������
			connector.commitOffsets();
		}
	}
}
