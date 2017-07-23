package com.cloudera.sa.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class SendGetTest 
{
	public static void main( String[] args ) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException
    {
    	if (args.length == 0) {
    		System.out.println("SendGetTest {host} {queue}");
    		return;
    	}
    	
    	String host = args[0];
    	String queue = args[1];
    	
        send(host, queue);
        
        recieve(host, queue);
    }

    public static void recieve(String host, String queue) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
    	ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queue, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, true, consumer);

        while (true) {
          QueueingConsumer.Delivery delivery = consumer.nextDelivery();
          String message = new String(delivery.getBody());
          System.out.println(" [x] Received '" + message + "'");
        }
    }
    
	private static void send(String host, String queue) throws IOException {
		System.out.println( "RabbitMQ Send" );
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        channel.queueDeclare(queue, false, false, false, null);
        String message = "Hello World!";
        channel.basicPublish("", queue, null, message.getBytes());
        
        System.out.println(" [x] Sent '" + message + "'");
        
        channel.close();
        connection.close();
	}
}
