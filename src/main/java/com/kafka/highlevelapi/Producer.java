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
		//kafka的broker的信息
		props.put("metadata.broker.list", "10.249.73.142:9092,10.249.73.143:9092,10.249.73.144:9092");
		//-1:表示获得所有的replica的确认信息再返回,延迟最大,但是也没有完全消除丢失消息的风险,因为同步的replica的数据可能是1
		//0:producer不等待来自broker的确认信息,发完立刻返回,延迟最小同样丢数据风险也是最大
		//1:表示获得leader replica的确认信息再返回
		props.put("request.required.acks", "1");
		//当request.required.acks设置为-1时,min.insync.replicas指定replicas的最小数目(必须确认每个replica的写数据都是成功的),如果这个数目没有达到,producer会产生异常.
		//props.put("min.insync.replicas","2");
		
		//Key的序列化类,如果没给这项,默认情况是和消息一致
		//props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		
		//serializer.class消息的序列化类,默认情况是"kafka.serializer.DefaultEncoder",该类的输入时byte[],然后返回byte[]
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		ProducerConfig pConfig = new ProducerConfig(props);
		
		kafka.javaapi.producer.Producer<String, String> kafkaProducer = new kafka.javaapi.producer.Producer<String, String>(pConfig);
		
		//1:消息的topic 2:Key,根据Key进行计算,分配到不同的Partition上 3:Value
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
