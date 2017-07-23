package com.cloudera.sa.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQReceiver {
	private static final int MULTI_ACK_COUNT = 1000;

	public static void main(String[] args) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		if (args.length == 0) {
			System.out.println("RabbitMQReciever {host} {port} {queue} {durable} {multiack-count}");
			return;
		}
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		String queue = args[2];
		
		int multiackCount = MULTI_ACK_COUNT;
		boolean durable = Boolean.parseBoolean(args[3]);
		
		if (args[4] != null)
			multiackCount = Integer.parseInt(args[4]);
		
		Connection connection = null;
		Channel channel = null;
		QueueingConsumer.Delivery delivery = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(host);
			factory.setPort(port);
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.queueDeclare(queue, durable, false, false, null);
			channel.basicQos(0, 0, false);

			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queue, false, consumer);
			
			long counter = 0;
			

			while (true) {
			  delivery = consumer.nextDelivery();
			  //String message = new String(delivery.getBody());
			  if (counter % multiackCount == 0)
				  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
			  
			  counter++;
			 // System.out.println(" [" + counter +"] Received '" + message + "'"); 
			}


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// multi acking the last messages if any
			if (delivery != null && channel != null) 
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);

			if (channel != null) channel.close();
			if (connection != null) connection.close();
		}
	}
}
