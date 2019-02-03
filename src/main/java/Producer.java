import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage : kafka-producer.jar inputFilePath");
        } else {
            System.out.println("Starting Streaming Job..");

            String inputFilePath = args[0];

            FileInputStream inputStream = null;
            Scanner sc = null;

            try {
                inputStream = new FileInputStream(inputFilePath);
                sc = new Scanner(inputStream, "UTF-8");
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    System.out.println(line);


                    String topicName = "sample";

                    //Configure the Producer
                    Properties configProperties = new Properties();
                    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");

                    org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

                    ObjectMapper objectMapper = new ObjectMapper();

                    JsonNode jsonNode = objectMapper.valueToTree(line);
                    ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
                    producer.send(rec);
                    producer.close();

                }
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (sc != null) {
                    sc.close();
                }
            }

        }
    }

}