package com.kafka.lowerlevelapi;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class LowerLevelConsumer {
	
	private List<String> m_replicaBrokers = null;
	
	public LowerLevelConsumer() {
		m_replicaBrokers = new ArrayList<String>();
	}
	
	
	public static void main(String[] args) {
		//��Ҫ���ѵ���Ϣ������
		String topic = "KAFKA_1";
		//�˿�
		int port = 9092;
		//partition�ı��
		int partitionNum = 1;
		//kafka��Ⱥ���б�
		List<String> brokersList = new ArrayList<String>();
		brokersList.add("10.249.73.142");
		brokersList.add("10.249.73.143");
		brokersList.add("10.249.73.144");
		
		int fetchSize = 10000;
		
		
		LowerLevelConsumer lowerLevelConsumer = new LowerLevelConsumer();
		lowerLevelConsumer.run(brokersList, port, topic, partitionNum, fetchSize);
	}
	
	/**
	 * 
	 * @param brokers
	 * @param port
	 * @param topic
	 * @param partitionId
	 * @param fetchSize ÿ��fetch���ݵ�����
	 */
	@SuppressWarnings("static-access")
	public void run(List<String> brokers, int port, String topic, int partitionId, int fetchSize) {
		PartitionMetadata partitionMetadata = findLeader(brokers, port, topic, partitionId);
		if(null == partitionMetadata) {
			return;
		}
		
		if(null == partitionMetadata.leader()) {
			return;
		}
		
		String partitionLeaderBroker = partitionMetadata.leader().host();
		String clientName = "Client_" + topic + "_" + partitionId;
		
		SimpleConsumer simpleConsumer = new SimpleConsumer(partitionLeaderBroker, port, 10000, 64 * 1024, clientName);
		long readOffset = getFirstOffset(simpleConsumer, topic, partitionId, clientName);
		
		//�쳣����,�����˳�
		int numErrors = 0;
		
		while(fetchSize > 0) {
			if(null == simpleConsumer) {
				simpleConsumer = new SimpleConsumer(partitionLeaderBroker, port, 10000, 64 * 1024, clientName);
			}
			
			FetchRequest request = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partitionId, readOffset, fetchSize).build();
			FetchResponse response = simpleConsumer.fetch(request);
			
			//������response����Error
			if(response.hasError()) {
				numErrors++;
				//�õ�Error code
				short code = response.errorCode(topic, partitionId);
				System.err.println("Error fetching data from the broker : " + partitionLeaderBroker + " Reason : " + code);
				//��������5��,ֱ���˳�
				if(numErrors > 5) {
					break;
				}
				
				//OffsetԽ��,�����������Ѿ������,����offset̫��,ѹ��û����ô������
				if(code == ErrorMapping.OffsetOutOfRangeCode()) {
					//�����µ�offset��ʼ��ȡ����
					readOffset = getLastOffset(simpleConsumer, topic, partitionId, clientName);
					continue;
				}
				
				simpleConsumer.close();
				simpleConsumer = null;
				try {
					//�������������Error,��ô��Ϊ��Partition���ڵ�broker崻�,��ô�����Ҫ�л����µ�brokerȥ
					partitionLeaderBroker = findNewLeader(partitionLeaderBroker, topic, partitionId, port);
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
				
				continue;
			}
			
			//Error count ��0
			numErrors = 0;
			
			//��ȡ����message�ĸ���
			long numRead = 0;
			
			for (MessageAndOffset messageAndOffset : response.messageSet(topic, partitionId)) {
				long currentOffset = messageAndOffset.offset();
				if(currentOffset < readOffset) {
					 System.err.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					 continue;
				}
				
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
				numRead++;
				fetchSize--;
			}
			
			if(numRead == 0) {
				try {
					Thread.currentThread().sleep(1000l);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		if(null!=simpleConsumer) {
			simpleConsumer.close();
		}
	}
	
	/**
	 * 
	 * @param a_oldLeader
	 * @param a_topic
	 * @param a_partition
	 * @param a_port
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("static-access")
	private String findNewLeader(String a_oldLeader, String topic, int partitionId, int port) throws Exception  {
		
		for(int i=0; i<3; i++) {
			boolean isGoToSleep = false;
			PartitionMetadata partitionMetadata = findLeader(m_replicaBrokers, port, topic, partitionId);
			if(null == partitionMetadata) {
				isGoToSleep = true;
			} else if(null == partitionMetadata.leader()) {
				isGoToSleep = true;
			} else if(a_oldLeader.equalsIgnoreCase(partitionMetadata.leader().host()) && i == 0) {
				isGoToSleep = true;
			} else {
				return partitionMetadata.leader().host();
			}
			
			if(isGoToSleep) {
				try {
					Thread.currentThread().sleep(1000l);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
	
	/**
	 * 
	 * @param simpleConsumer
	 * @param topic
	 * @param partitionId
	 * @param clientName
	 * @return
	 */
	private long getLastOffset(SimpleConsumer simpleConsumer, String topic, int partitionId, String clientName) {
		return getOffset(simpleConsumer, topic, partitionId, clientName, kafka.api.OffsetRequest.LatestTime());
	}
	
	/**
	 * 
	 * @param simpleConsumer
	 * @param topic
	 * @param partitionId
	 * @param clientName
	 * @return
	 */
	private long getFirstOffset(SimpleConsumer simpleConsumer, String topic, int partitionId, String clientName) {
		return getOffset(simpleConsumer, topic, partitionId, clientName, kafka.api.OffsetRequest.EarliestTime());
	}
	
	/**
	 * 
	 * @param simpleConsumer
	 * @param topic
	 * @param partitionId
	 * @param clientName
	 * @param witchTime
	 * @return
	 */
	private long getOffset(SimpleConsumer simpleConsumer, String topic, int partitionId, String clientName, long witchTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(witchTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
		if(response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partitionId) );
            return 0;
		}
		
		long[] offsets = response.offsets(topic, partitionId);
        return offsets[0];
	}
	
	/**
	 * Ѱ��ָ��topic��ָ��Partition��leader���ڵĻ���
	 * @param brokers
	 * @param port
	 * @param topic
	 * @param partitionId
	 * @return
	 */
	private PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partitionId) {
		
		SimpleConsumer simpleConsumer = null;
		PartitionMetadata returnPartitionMetadata = null;
		boolean isBreak = false;
		for (String broker : brokers) {
			if(isBreak) {
				break;
			}
			
			try {
				simpleConsumer = new SimpleConsumer(broker, port, 10000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest request = new TopicMetadataRequest(topics);
				TopicMetadataResponse response = simpleConsumer.send(request);
				
				List<TopicMetadata> metaDatas = response.topicsMetadata();
				for (TopicMetadata topicMetadata : metaDatas) {
					if(isBreak) {
						break;
					}
					
					for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
						if(partitionMetadata.partitionId() == partitionId) {
							returnPartitionMetadata = partitionMetadata;
							isBreak = true;
							break;
						}
					}
				}
			} catch (Exception e) {
				System.err.println("Error communicating with broker [" + broker + "] to find leader for [" + topic + ", " + partitionId + "] Reason : " + e);
			} finally {
				if(null!=simpleConsumer) {
					simpleConsumer.close();
				}
			}
		}
		
		//��Partition��replica���ڵ�broker�洢����
		if(null!=returnPartitionMetadata) {
			m_replicaBrokers.clear();
			for (Broker broker : returnPartitionMetadata.replicas()) {
				m_replicaBrokers.add(broker.host());
			}
		}
		
		return returnPartitionMetadata;
	}

}
