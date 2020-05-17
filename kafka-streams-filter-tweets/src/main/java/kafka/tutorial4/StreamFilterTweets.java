package kafka.tutorial4;


import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {
    public static void main(String[] args) {
        //create properties
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic=streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream=inputTopic.filter(
                (k,jsonTweet)-> ExtractUserFollowersfromTweet(jsonTweet) >10000
        );
        filteredStream.to("important-tweets");

        //build the topology
        KafkaStreams kafkaStreams= new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser=new JsonParser();

    private static Integer ExtractUserFollowersfromTweet(String twitterJson){
        try{
            return jsonParser.parse(twitterJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch(NullPointerException e){
            return 0;
        }
    }
}
