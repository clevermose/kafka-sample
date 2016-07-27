package com.kafka.highlevelapi;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer {
	
	private static final String TOPIC = "KAFKA_1";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		//kafka��broker����Ϣ
		props.put("metadata.broker.list", "10.249.73.142:9092,10.249.73.143:9092,10.249.73.144:9092");
		//-1:��ʾ������е�replica��ȷ����Ϣ�ٷ���,�ӳ����,����Ҳû����ȫ������ʧ��Ϣ�ķ���,��Ϊͬ����replica�����ݿ�����1
		//0:producer���ȴ�����broker��ȷ����Ϣ,�������̷���,�ӳ���Сͬ�������ݷ���Ҳ�����
		//1:��ʾ���leader replica��ȷ����Ϣ�ٷ���
		props.put("request.required.acks", "1");
		//��request.required.acks����Ϊ-1ʱ,min.insync.replicasָ��replicas����С��Ŀ(����ȷ��ÿ��replica��д���ݶ��ǳɹ���),��������Ŀû�дﵽ,producer������쳣.
		//props.put("min.insync.replicas","2");
		
		//Key�����л���,���û������,Ĭ������Ǻ���Ϣһ��
		//props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		
		//serializer.class��Ϣ�����л���,Ĭ�������"kafka.serializer.DefaultEncoder",���������ʱbyte[],Ȼ�󷵻�byte[]
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		ProducerConfig pConfig = new ProducerConfig(props);
		
		kafka.javaapi.producer.Producer<String, String> kafkaProducer = new kafka.javaapi.producer.Producer<String, String>(pConfig);
		
		//1:��Ϣ��topic 2:Key,����Key���м���,���䵽��ͬ��Partition�� 3:Value
		KeyedMessage<String, String> msg_1 = new KeyedMessage<String, String>(TOPIC, "1", "msg_1");
		KeyedMessage<String, String> msg_2 = new KeyedMessage<String, String>(TOPIC, "2", "msg_2");
		KeyedMessage<String, String> msg_3 = new KeyedMessage<String, String>(TOPIC, "3", "msg_3");
		
		List<KeyedMessage<String, String>> messages = new LinkedList<KeyedMessage<String,String>>();
		messages.add(msg_2);
		messages.add(msg_3);
		
		System.out.println("send single message.");
		kafkaProducer.send(msg_1);
		
		System.out.println("send multi-messages.");
		kafkaProducer.send(messages);
		
		kafkaProducer.close();
		System.out.println("over...");
	}
}
