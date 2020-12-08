package dk.dd.rabbit;
/*
 * Message Consumer
 *
 * 1) Creates  a queue, if it is not yet created
 * 2) Registers for notification of messages sent to its ID
 */
import com.rabbitmq.client.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class RabbitApplication
{
    private final static String QUEUE_NAME = "test_queue";
    //Variables for benchmarks
    private static boolean initiated = false;
    private static int iterationsLeft = 0;
    private static int count = 0;


    public static void main(String[] args) throws Exception
    {
        SpringApplication.run(RabbitApplication.class, args);
        connectQueue();
    }

    public static void connectQueue() throws Exception
    {

        // Same as the producer: tries to create a queue, if it wasn't already created
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Register for a queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");


        // Get notified, if a message for this receiver arrives
        DeliverCallback deliverCallback = (consumerTag, delivery) ->
        {
            count++;
            String message = new String(delivery.getBody(), "UTF-8");

            if(!initiated){
                initiated = true;


                iterationsLeft = Integer.parseInt(message.split(",")[0])-1;
            } if (iterationsLeft == 0){
                long endTime = System.nanoTime();
                long deltaTime = endTime-Long.valueOf(message.split(",")[1]);
                double deltaTimeMilis = deltaTime/1_000_000D;

//                System.out.println("Total time elapsed: " + deltaTimeMilis + "ms");
//                System.out.println("Delta nanoTime: " + deltaTime + "ns");
//                System.out.println("Avg. time pr. call: " + deltaTimeMilis/count + "ms\n");
                System.out.println(deltaTimeMilis);

                initiated = false;
                count = 0;
//                results.add(deltaTimeMilis/count);
            } else{
                iterationsLeft--;
            }
//            System.out.println(" [x] Received '" + message + "'");

        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});


    }
}

