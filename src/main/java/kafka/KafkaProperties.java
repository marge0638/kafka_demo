package kafka;

public interface KafkaProperties {
	final static String zkConnect = "localhost:2181";
	final static String groupId = "group1";
	final static String topic = "test_kafka_1";
	final static String kafkaServerURL = "localhost";
	final static int kafkaServerPort = 9092;
	final static int kafkaProducerBufferSize = 64*1024;
	final static int connectionTimeOut = 20000;
	final static int reconnectInterval = 10000;
	final static String clientId = "SimpleConsumerDemoClient";
}
