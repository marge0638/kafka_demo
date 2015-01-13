package kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.api.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class SimplestKafkaConsumer {
	public static void main(String[] args) throws UnsupportedEncodingException {
		String broker = "localhost";
		int port = Integer.parseInt("9092");
		int partition = Integer.parseInt("0");
		String topic = "test_kafka_1";
		String clientName = "simple_client";
		long fromOffset = Integer.parseInt("5");
		long endOffset = Integer.parseInt("10");
		SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 64*1024, clientName);
		long firstOffset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
		long lastOffset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
		
		System.out.println("******************Get message from begining to end***************************");
		long firstOffset_tmp = firstOffset;
		while (firstOffset_tmp < lastOffset) {
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, firstOffset_tmp, 10000).build();
			FetchResponse fetchResponse = consumer.fetch(req);
			
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)){
				firstOffset_tmp = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				
				byte[] bytes = new byte[(payload.limit())];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				lastOffset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
			}
			
		}
		
		System.out.println("********************Get message from 5 to 10*********************************");
		if (fromOffset < firstOffset) {
			fromOffset = firstOffset;
		}
		if (endOffset > lastOffset) {
			endOffset = lastOffset;
		}
		while (fromOffset <= endOffset) {
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, fromOffset, 10000).build();
			FetchResponse fetchResponse = consumer.fetch(req);
			
			for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)){
				fromOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[(payload.limit())];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				if (messageAndOffset.offset() >= endOffset) { 
					break;
				}
			}
		}
		if(consumer != null) {
			consumer.close();
		}
	}
	
	public static long getOffset(SimpleConsumer consumer, String a_topic, int a_partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(a_topic, a_partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(a_topic, a_partition));
			return 0;
		}
		long[]  offsets = response.offsets(a_topic, a_partition);
		return offsets[0];
	}
	
}
