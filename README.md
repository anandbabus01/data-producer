# data-producer

This Kafka producer reads from huge json files given in yelp data set and writing to separate kafka topics

Producer is not parallel processing, it linear process, once one file pushed , it will go next file and push to kafka brokers

before run this, create kafka topics using script, run this script in $KAFKA_HOME:
create-kafka-topics.sh

Please use below command to build jar:
mvn clean install 

use below script to run this script:

java -cp com.producer-1.0-SNAPSHOT-jar-with-dependencies.jar Driver < unzipped yelp dataset directory path >

- after one hour, we will be able to run Spark Streaming Consumer


