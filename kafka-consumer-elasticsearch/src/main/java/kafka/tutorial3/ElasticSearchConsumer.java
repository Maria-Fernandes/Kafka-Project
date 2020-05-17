package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

        String hostname = "kafka-course-2479748975.us-west-2.bonsaisearch.net";
        String username = "ogq1epvtni";
        String password = "6054pxv71e";

        // dont do if you run a local elastic search
        final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider);
                    }
                });
        RestHighLevelClient client=new RestHighLevelClient(builder);

        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        final Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        // create consumer configs

        String groupId="kafka-demo-elasticsearch";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"200");


        // create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }

    private static JsonParser jsonParser=new JsonParser();

    private static String ExtractIdfromTwitterrecord(String twitterJson){
        return jsonParser.parse(twitterJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client=createClient();


        String topic="twitter_tweets";

        KafkaConsumer<String,String> consumer=createConsumer(topic);

        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            Integer count=records.count();
            logger.info("Recieved"+count+" records");

            BulkRequest bulkRequest=new BulkRequest();

            for(ConsumerRecord<String,String> record:records){
                // 2 strategies
                // kafka generic id
                // String id= record.topic()+"_"+record.partition()+"_"+record.offset();

                //feed specific twitter id
                try{
                    String i=ExtractIdfromTwitterrecord(record.value());
                    String jsonString=record.value();
                    IndexRequest indexRequest=new IndexRequest("twitter", "tweets",i)
                            .source(jsonString, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch(NullPointerException e){
                    logger.info("skipping data "+record.value());
                }

//                IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
//                String id=indexResponse.getId();
//                logger.info(id);
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
            if(count>0){
                BulkResponse bulkItemResponses=client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Commiting offsets");
                consumer.commitSync();
                logger.info("Committed offsets");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


//        client.close();
    }
}
