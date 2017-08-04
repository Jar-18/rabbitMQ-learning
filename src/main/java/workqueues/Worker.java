package workqueues;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


public class Worker {

	  private final static String TASK_QUEUE_NAME = "hello";

	  public static void main(String[] argv)
	      throws java.io.IOException,
	             java.lang.InterruptedException, TimeoutException {

	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    Connection connection = factory.newConnection();
	    final Channel channel = connection.createChannel();

//	    int prefetchCount = 1;
//	    channel.basicQos(prefetchCount);

	    
	    channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    
//	    Consumer consumer = new DefaultConsumer(channel) {
//	    	  @Override
//	    	  public void handleDelivery(String consumerTag, Envelope envelope,
//	    	                             AMQP.BasicProperties properties, byte[] body)
//	    	      throws IOException {
//	    	    String message = new String(body, "UTF-8");
//	    	    System.out.println(" [x] Received '" + message + "'");
//	    	  }
//	    	};
//	    	channel.basicConsume(QUEUE_NAME, true, consumer);
	    	
	    	final Consumer consumer = new DefaultConsumer(channel) {
	    		  @Override
	    		  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	    		    String message = new String(body, "UTF-8");

	    		    System.out.println(" [x] Received '" + message + "'");
	    		    try {
	    		      doWork(message);
	    		    } catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
	    		      System.out.println(" [x] Done");
	    		      channel.basicAck(envelope.getDeliveryTag(), false);
	    		    }
	    		  }
	    		};
	    		boolean autoAck = false; // acknowledgment is covered below
	    		channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
	    
    }
	  
	  private static void doWork(String task) throws InterruptedException {
		    for (char ch: task.toCharArray()) {
		        if (ch == '.') Thread.sleep(1000);
		    }
		}    
	
}
