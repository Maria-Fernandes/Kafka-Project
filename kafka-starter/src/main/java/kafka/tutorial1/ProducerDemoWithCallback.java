package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // creating producer properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // creating producer

        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);


        for(int i=0;i<10;i++){
            // create a producer record

            ProducerRecord<String,String> record=new ProducerRecord<String, String>("first-topic","Welcome Maria"+Integer.toString(i));

            // send data- asynchronous
            // Command + P -> check it out
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
            });
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
