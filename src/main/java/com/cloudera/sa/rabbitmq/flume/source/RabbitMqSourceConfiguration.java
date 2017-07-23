package com.cloudera.sa.rabbitmq.flume.source;

public class RabbitMqSourceConfiguration {

	public static final String HOST = "host";
	
	public static final String QUEUE = "queue";
	
	public static final String PORT = "port";
	
	public static final String EXCHANGE_NAME = "exchangeName";

	public static final String EXCHANGE_TYPE = "exchangeType";

	public static final String ROUTING_KEY = "routingKey";
	
	public static final String USERNAME = "userName";
	
	public static final String VIRTUAL_HOST = "virtualHost";

	public static final String PASSWORD_FILE = "passwordFile";

	public static final String BATCH_SIZE = "batchSize";
	public static final int BATCH_SIZE_DEFAULT = 100;
	
	public static final String PREFETCH_COUNT = "prefetchCount";
	public static final int PREFETCH_COUNT_DEFAULT = 2000;

	public static final String ERROR_THRESHOLD = "errorThreshold";
	public static final int ERROR_THRESHOLD_DEFAULT = 10;

	
	public static final String POLL_TIMEOUT = "pollTimeout";
	public static final long POLL_TIMEOUT_DEFAULT = 1000L;

	public static final Integer PORT_DEFAULT = 5672;

	public static final String DURABLE = "durable";	
	public static final String EXCHANGE_DURABLE = "exchangeDurable";	

}