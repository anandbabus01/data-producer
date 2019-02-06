# data-producer

This Kafka producer reads from huge json files given in yelp data set and writing to separate kafka topics

Producer is not parallel processing, it linear process, once one file pushed , it will go next file and push to kafka brokers

If kafka not listening to localhost:9092, please update kafka server ip address in Producer.java
line#53
configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

before run this, create kafka topics using script, run this script in $KAFKA_HOME:
bash create-kafka-topics.sh

Please use below command to build jar:
mvn clean install 

use below script to run this script:

java -cp com.producer-1.0-SNAPSHOT-jar-with-dependencies.jar Driver < unzipped yelp dataset directory path 'e.g. /home/ubuntu/yelp_dataset/'  >

Note:
Producer is not parallel processing, it linear process, once one file pushed , it will go next file and push to kafka brokers
after one hour, we will be able to run Spark Streaming Consumer

I advise to create smaller files of each json to test this code faster like below example:
head -10000 business.json > new-dir/business.json

this jars can be dockerized easy way by these steps:
https://runnable.com/docker/java/dockerize-your-java-application


