package com.cloudera.sa.rabbitmq.flume.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.cloudera.sa.rabbitmq.flume.source.QueueingConsumer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RabbitMqFlumeSource extends AbstractPollableSource {
	private static Logger logger = LoggerFactory
			.getLogger(RabbitMqFlumeSource.class);

	// setup by constructor

	// setup by configuration
	private int batchSize;
	private String host;
	private String queue;
	private int port;
	private boolean durable;
	private boolean exchangeDurable;
	private String exchangeName;
	private String exchangeType;
	private String routingKey;
	private Optional<String> userName;
	private Optional<String> password;
	private Optional<String> virtualHost;
	private SourceCounter sourceCounter;
	private int errorThreshold;
	
	QueueingConsumer consumer;
	Channel channel;

	private int RabbitMqExceptionCounter;

	public RabbitMqFlumeSource() {
	}

	@Override
	protected void doConfigure(Context context) throws FlumeException {
		sourceCounter = new SourceCounter(getName());

		host = context.getString(RabbitMqSourceConfiguration.HOST, "").trim();

		port = context.getInteger(RabbitMqSourceConfiguration.PORT,
				RabbitMqSourceConfiguration.PORT_DEFAULT);
		
		queue = context.getString(RabbitMqSourceConfiguration.QUEUE, "").trim();
		
		durable = context.getBoolean(RabbitMqSourceConfiguration.DURABLE, false);
		exchangeDurable = context.getBoolean(RabbitMqSourceConfiguration.EXCHANGE_DURABLE, false);
		
		exchangeName = context.getString(RabbitMqSourceConfiguration.EXCHANGE_NAME, "").trim();
		exchangeType = context.getString(RabbitMqSourceConfiguration.EXCHANGE_TYPE, "topic").trim();
		
		// default routing key to queue name
		routingKey = context.getString(RabbitMqSourceConfiguration.ROUTING_KEY, queue).trim();
		
		virtualHost = Optional.fromNullable(context
				.getString(RabbitMqSourceConfiguration.VIRTUAL_HOST));

		batchSize = context.getInteger(RabbitMqSourceConfiguration.BATCH_SIZE,
				RabbitMqSourceConfiguration.BATCH_SIZE_DEFAULT);
		
		errorThreshold = context.getInteger(
				RabbitMqSourceConfiguration.ERROR_THRESHOLD,
				RabbitMqSourceConfiguration.ERROR_THRESHOLD_DEFAULT);

		userName = Optional.fromNullable(context
				.getString(RabbitMqSourceConfiguration.USERNAME));


		String passwordFile = context.getString(
				RabbitMqSourceConfiguration.PASSWORD_FILE, "").trim();

		if (passwordFile.isEmpty()) {
			password = Optional.of("");
		} else {
			try {
				password = Optional.of(Files.toString(new File(passwordFile),
						Charsets.UTF_8).trim());
			} catch (IOException e) {
				throw new FlumeException(String.format(
						"Could not read password file %s", passwordFile), e);
			}
		}
		
		assertNotEmpty(host, String.format(
				"host is empty. This is specified by %s",
				RabbitMqSourceConfiguration.HOST));

		assertNotEmpty(queue, String.format(
				"queue is empty. This is specified by %s",
				RabbitMqSourceConfiguration.QUEUE));

		Preconditions.checkArgument(batchSize > 0,
				"Batch size must be greater " + "than 0");

	}

	private void assertNotEmpty(String arg, String msg) {
		Preconditions.checkArgument(!arg.isEmpty(), msg);
	}

	long counter = 0;
	
	@Override
	protected synchronized Status doProcess() throws EventDeliveryException {
		boolean error = true;
		long lastDeliveryTag = 0;
		int numberMsgs = 0;
		
		try {
			if (consumer == null) {
				consumer = createConsumer();
			}
			
			numberMsgs = consumer.getLocalQueueSize();
			
			if (logger.isDebugEnabled()) 
			  logger.debug("queue size of queue: " + queue + " is " + numberMsgs);
			
			if (numberMsgs == 0) {
				error = false;
				return Status.BACKOFF;
			}
			
			if (numberMsgs > batchSize)
				numberMsgs = batchSize;
			
			if (logger.isDebugEnabled()) 
			  logger.debug("processing " + numberMsgs + " messages in queue " + queue);
			
			ArrayList<Event> events = new ArrayList<Event>(numberMsgs);
			
			QueueingConsumer.Delivery delivery = null;
			
			for (int i = 0; i < numberMsgs; i++) {
				delivery = consumer.nextDelivery();
				if (delivery != null) {
					SimpleEvent event = new SimpleEvent();
					byte[] body = delivery.getBody();
					
					event.setBody(body);
					events.add(event);
					
					lastDeliveryTag = delivery.getEnvelope().getDeliveryTag();
				} else {
					if (i == 0) {
						error = false;
						return Status.BACKOFF;
					}
					break;
				}
			}
		
			sourceCounter.incrementAppendBatchReceivedCount();
			sourceCounter.addToEventReceivedCount(events.size());
			
			getChannelProcessor().processEventBatch(events);
			error = false;
			sourceCounter.addToEventAcceptedCount(events.size());
			sourceCounter.incrementAppendBatchAcceptedCount();
			
			return Status.READY;
		} 
		catch (ChannelException channelException) {
			logger.warn("Error appending event to channel. "
					+ "Channel might be full. Consider increasing the channel "
					+ "capacity or make sure the sinks perform faster.",
					channelException);

		} catch (Exception exception) {
			logger.warn("Failed to consume events", exception);
			if (++RabbitMqExceptionCounter > errorThreshold) {
			
				if (consumer != null) {
					logger.warn("Exceeded Exception threshold, closing consumer");	
					try {
						if (channel.isOpen()) channel.close();
					} catch (IOException e) {
						logger.error("failed to close RabbitMQ channel", e);
					}
					consumer = null;
				}
			}
		} catch (Throwable throwable) {
			logger.error("Unexpected error processing events", throwable);
			if (throwable instanceof Error) {
				throw (Error) throwable;
			}
		} finally {	
		  if (consumer != null) {
		    if (!error) {				
		      RabbitMqExceptionCounter = 0;
		      if (lastDeliveryTag != 0) {
		        try {
		          channel.basicAck(lastDeliveryTag, true);
		          lastDeliveryTag = 0;
		        } catch (IOException e) {
		        	logger.error("failed to ack", e);
		        }
		      }
		    } else {
		      if (lastDeliveryTag != 0) {
		        try {
		          channel.basicNack(lastDeliveryTag, true, true);
		          lastDeliveryTag = 0;
		        } catch (IOException e) {
		        	logger.error("failed to nack", e);
		        }
		      }
		    }
		  }
		}
		return Status.BACKOFF;
	}

	@Override
	protected synchronized void doStart() {
		try {
			consumer = createConsumer();
			RabbitMqExceptionCounter = 0;
			sourceCounter.start();
		} catch (Exception e) {
			throw new FlumeException("Unable to create consumer", e);
		}
	}

	@Override
	protected synchronized void doStop() {
		if (consumer != null) {			
			consumer = null;
		}
		try {
			channel.close();
		} catch (IOException e) {
			logger.error("failed to close channel", e);
		}

		sourceCounter.stop();
	}

	private QueueingConsumer createConsumer() throws IOException {

		logger.warn("Creating new consumer for queue:" + queue);
		ConnectionFactory factory = new ConnectionFactory();
		if (userName.isPresent() && !userName.get().isEmpty()) {
			logger.info("Username " + userName);
			factory.setUsername(userName.get());
		}
		if (password.isPresent() && !password.get().isEmpty()) {
			logger.info("password " + password);
			factory.setPassword(password.get());
		}
		if (virtualHost.isPresent() && !virtualHost.get().isEmpty()) {
			logger.info("virtualHost " + virtualHost);
			factory.setVirtualHost(virtualHost.get());
		}
		factory.setPort(port);
		factory.setHost(host);
		Connection connection;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);			
			channel.queueDeclare(queue, durable, false, false, null);
			channel.queueBind(queue, exchangeName, routingKey);
			channel.basicQos(batchSize);
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
	        channel.basicConsume(queue, false, consumer);
	        			
			RabbitMqExceptionCounter = 0;

			return consumer;
		} catch (IOException e) {
			logger.error("failed to configure queue or consumer", e);
			throw e;
		}

	}
}