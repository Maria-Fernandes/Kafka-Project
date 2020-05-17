package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger=LoggerFactory.getLogger(ProducerDemoKeys.class);

        // creating producer properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // creating producer

        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);


        for(int i=0;i<10;i++){
            // create a producer record

            String topic="second-topic";
            String key="id_"+Integer.toString(i);
            String value="Welcome Maria"+Integer.toString(i);


            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topic,key,value);

            // send data- asynchronous
            // Command + P -> check it out

            logger.info("Key "+key+" ");
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is successfully sent or an exception is thrown
                    if(e == null){
                        // record was successfully sent
                        logger.info("Recieved new metadata \n"+
                                "Topic "+ recordMetadata.topic()+"\n"+
                                "Partition "+ recordMetadata.partition()+"\n"+
                                "Offset "+ recordMetadata.offset()+"\n"+
                                "Timestamp "+ recordMetadata.timestamp()+"\n"
                        );
                    }
                    else{
                        logger.error("Error while producing data "+e);
                    }
                }
            }).get(); // block the send to make it synchronous - dont do in production
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
