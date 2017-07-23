package com.cloudera.sa.rabbitmq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MultiThreadedRabbitMQLoader {
	
	static ArrayList<String> records = new ArrayList<String>(); 
	
	static int liveThreads = 0;
	static long submitted = 0;
	
	static Logger logger = LoggerFactory.getLogger(MultiThreadedRabbitMQLoader.class);
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			System.out.println("BatchRabbitMQLoader {host} {port} {queue} {exchange} {routingKey} {durable (true or false)} {inputFile} {numOfThreads} {Total records per thread} {numOfRecordsPerSend}");
			return;
		}
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		String queue = args[2];
		String exchange = args[3];
		String routingKey = args[4];
		boolean durable = args[5].equals("true");
		String inputFile = args[6];
		int numOfThreads = Integer.parseInt(args[7]);
		int totalMsgPerThread = Integer.parseInt(args[8]);
		int numOfRecordsPerSend = Integer.parseInt(args[9]);
		
		// number of records per send must not be total messages per thread
		if (totalMsgPerThread < numOfRecordsPerSend)
		{
			System.out.println("setting number of records per send equals to pub per send");
			numOfRecordsPerSend = totalMsgPerThread;
		}
		
		loadInputFileIntoMemory(inputFile);
		
		for (int i = 0; i < numOfThreads; i ++) {
			Thread thread = new Thread( new SendRunnable(host, port, queue, exchange, routingKey, durable, totalMsgPerThread, numOfRecordsPerSend));
			liveThreads++;
			thread.start();
		}
		
		while (liveThreads > 0) {
			Thread.sleep(1000);
			System.out.println("Submitted: " + submitted);
		}
		return;
	}
	
	private static void loadInputFileIntoMemory(String inputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)));
		
		String line;
		while ((line = reader.readLine()) != null) {
			records.add(line);
		}
		reader.close();
	}

	static class SendRunnable implements Runnable {

		String queue;
		Channel channel;
		int totalMsgPerThread;
		int currentIndex = 0;
		int recordBatchSize = 0;
		int numOfRecordsPerSend;
		String exchange;
		String routingKey;
		
		public SendRunnable (String host,int port, String queue, String exchange, String routingKey, boolean durable, int totalMsgPerThread, int numOfRecordsPerSend) throws IOException {
			channel = getChannel(host, port,  queue,exchange, routingKey, durable);
			this.totalMsgPerThread = totalMsgPerThread;
			this.queue = queue;
			this.numOfRecordsPerSend = numOfRecordsPerSend;
			this.exchange = exchange;
			this.routingKey = routingKey;
		}
		
		public void run() {
			
			try {
				StringBuilder strBuilder = new StringBuilder();
				for (int i = 0; i < totalMsgPerThread; i++) {
						strBuilder.append(records.get(currentIndex++) + "\n");
						recordBatchSize++;
						
						if (recordBatchSize >= numOfRecordsPerSend) {
							channel.basicPublish(exchange, routingKey, null,strBuilder.toString().getBytes());
							strBuilder.setLength(0);
							//strBuilder = new StringBuilder();
							recordBatchSize = 0;
							submitted++;
							
							System.out.println("finished sending " + numOfRecordsPerSend + " messages");							
						}
					if (currentIndex >= records.size()) {
						currentIndex = 0;
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				liveThreads--;

				try {
					channel.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
	
	private static Channel getChannel(String host, int port, String queue, String exchange, String routingKey, boolean durable) throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchange, "topic", durable);
        channel.queueDeclare(queue, durable, false, false, null);
        channel.queueBind(queue, exchange, routingKey);
        return channel;
	}
}
