package kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.api.FetchRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class SimpleKafka {
	private List<String> m_replicaBrokers = new ArrayList<String>();
	
	public void SimpeKafka() {
		m_replicaBrokers  = new ArrayList<String>();
	}
	
	public static void main(String[] args) {
		SimpleKafka example = new SimpleKafka();
		//最大读取消息数量
		long maxReads = Long.parseLong("3");
		String topic = "test_kafka_1";
		int partition = Integer.parseInt("0");
		//broker节点ip
		List<String> seeds = new ArrayList<String>();
		seeds.add("localhost");
		//port
		int port = Integer.parseInt("9092");
		try{
			example.run(maxReads, topic, partition, seeds, port);
		} catch (Exception e) {
			System.out.println("Oops: " + e);
			e.printStackTrace();
		}
	}
	
	public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
		//获取指定topic partition的元数据
		/*PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();*/
		String leadBroker = "localhost";
		String clientName = "Client_" + a_topic + "_" + a_partition;
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 30000, 64*1024, clientName);
		long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
		int numErrors = 0;
		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 30000, 64*1024, clientName);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition, readOffset, 10000).build();
			FetchResponse  fetchResponse = consumer.fetch(req);
			if (fetchResponse.hasError()) {
				numErrors ++;
				short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker: " + leadBroker + "Reason" + code);
				if (numErrors > 5) {
					break;
				}
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
				continue;
			}
			numErrors = 0;
			
			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				numRead ++;
				a_maxReads--;
			}
			
			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
		
		if(consumer != null) {
			consumer.close();
		}
	}
	
	public PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetadata = null;
		loop : for(String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 30000, 64*1024, KafkaProperties.clientId);
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
				
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetadata = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communication with Broker ["+seed+"] to find Lead for ["+a_topic+","+a_partition+"] Reason: " +e);
			} finally {
				if(consumer != null) {
					consumer.close();
				}
			}
			if (returnMetadata != null) {
				m_replicaBrokers.clear();
				for (kafka.cluster.Broker replica : returnMetadata.replicas()) {
					m_replicaBrokers.add(replica.host());
				}
			}
			
		}
		return returnMetadata;
	}
	
	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if(response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.out.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}
}
