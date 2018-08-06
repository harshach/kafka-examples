This application consumes messages from kafka topic. Make sure Kafka and zookeper are up and running on the machine we want to run kafka consumer.

Instructions for Kafka setup : http://kafka.apache.org/documentation.html#quickstart

Build the Jar : gradle clean build fatjar -x test

Run the application :

java -jar [jarForApplication] [topicName] [groupName] [brokerHostAndPort] [offset]

We need to pass 4 command line arguements : topicName, groupName, brokerHostAndPort and Offset to start the application.

Ex : java -jar build/9984d94/kafka-consumer-9984d94-all-1.0-SNAPSHOT.jar test-topic group1 localhost:9092 100

After you start the application, you will see the current offset and updated offset. You can type exit anytime to quit the consumer.
