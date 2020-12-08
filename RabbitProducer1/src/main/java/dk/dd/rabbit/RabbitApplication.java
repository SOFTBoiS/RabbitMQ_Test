package dk.dd.rabbit;
/*
 * Message Producer
 *
 * Produces a simple message, which will be delivered to a specific consumer
 * 1) Creates a queue
 * 2) Sends the message to it
 */
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.json.GsonJsonParser;
import com.google.gson.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@SpringBootApplication
public class RabbitApplication
{
    private final static Gson gson = new Gson();
    private final static String QUEUE = "test_queue";
    private final static String EXCHANGE_NAME = "queue";

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RabbitApplication.class, args);

        createQueue();
        Order order = new Order();
        String json = gson.toJson(order);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");


        try ( Connection connection = factory.newConnection();
              Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", false);
            channel.queueDeclare(QUEUE, false, false, false, null);
            channel.queueBind(QUEUE, EXCHANGE_NAME, QUEUE);
            String message = "";

            int[] iterations = {1,10,100,1_000,10_000,100_000};

            for (int iteration : iterations) {
                for (int i = 0; i<10; i++){
                    Thread.sleep(1000);
                    System.out.println("Iteration set " + iteration + " run " + i + "th time");
                    long nanoTime = System.nanoTime();

                    for (int j = 0; j < iteration; j++) {
                        message = iteration - j + "," + nanoTime + "," + order.toJson();
                        channel.basicPublish(EXCHANGE_NAME, QUEUE, MessageProperties.TEXT_PLAIN, message.getBytes("UTF-8"));
                    }

                }

            }

        }


    }

    public static void createQueue() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try ( Connection connection = factory.newConnection();
              Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", false);
            channel.queueDeclare(QUEUE, false, false, false, null);
            channel.queueBind(QUEUE, EXCHANGE_NAME, QUEUE);

        }
    }


    //
    private static class Order{
        private int ID = 0;
        private int price = 100;
        private String status = "Sent";
        private String customer_id = "Adam";
        private List<Integer> item_id;

        public Order() {
            item_id = new ArrayList<Integer>();
            item_id.add(1);
            item_id.add(2);
            item_id.add(3);
            item_id.add(4);
        }

        public String toJson() {
            return gson.toJson(this);
        }
    }
}