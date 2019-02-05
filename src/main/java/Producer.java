import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public void producer(String input_file_path) throws IOException {

        String inputFilePath = input_file_path;

        File dir = new File(inputFilePath);
        FileFilter fileFilter = new WildcardFileFilter("*.json", IOCase.INSENSITIVE); // For taking both .JPG and .jpg files (useful in *nix env)
        File[] fileList = dir.listFiles(fileFilter);
        if (fileList.length > 0) {
            /** The oldest file comes first **/
            Arrays.sort(fileList, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
            for (int i = 0; i < fileList.length; i++) {
                if (fileList[i].isFile()) {

                    String singleFile = inputFilePath + fileList[i].getName();
                    System.out.println(singleFile);
                    FileInputStream inputStream = null;
                    Scanner sc = null;

                    String topicName = fileList[i].getName();
                    if (topicName.indexOf(".") > 0)
                        topicName = topicName.substring(0, topicName.lastIndexOf("."));
                    System.out.println(topicName);

                    try {
                        inputStream = new FileInputStream(singleFile);
                        sc = new Scanner(inputStream, "UTF-8");
                        while (sc.hasNextLine()) {
                            String line = sc.nextLine();
                            System.out.println(line);


                            //Configure the Producer
                            Properties configProperties = new Properties();
                            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//                    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");

                            org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

//                    ObjectMapper objectMapper = new ObjectMapper();

//                    JsonNode jsonNode = objectMapper.valueToTree(line);
                            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
                            producer.send(rec);
                            producer.close();

                        }
                    } catch (IOException e) {
                        e.printStackTrace();
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
    }
}
