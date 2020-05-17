package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread(){

    }

    public void run(){
        Logger logger= LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String groupId="my-fifth-application";
        String topic="second-topic";
        CountDownLatch latch=new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable("localhost:9092",topic,groupId,latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));


        try {
            latch.await();
        }catch(InterruptedException e){
            logger.info("Application got interrupted",e);
        }
        finally{
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        final Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServer,String topic,String groupId,CountDownLatch latch){
            this.latch=latch;

            // create consumer configs

            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


            // create consumer
            consumer= new KafkaConsumer<String, String>(properties);

            // subscribe to the topics
            consumer.subscribe(Arrays.asList(topic)); // we can specify a single or list of topics
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
//                        logger.info("Key: " + record.key() + " , Value: " + record.value() + " ");
//                        logger.info("Partition: " + record.partition() + " , Offset: " + record.offset() + " ");
                    }
                }
            }catch(WakeupException e){
                logger.info(" Received shutdown signal!");
            }
            finally{
                consumer.close();
                // tell our main code we are done with the consumer
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup method is special method used to interrupt consumer.poll()
            // it will throw exception WakeUpException
            consumer.wakeup();
        }
    }
}
