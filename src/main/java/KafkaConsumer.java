import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

public class KafkaConsumer {
    private static Scanner in;

    public static void main(String[] args) throws InterruptedException {
        if(args.length != 4) {
            System.out.println("TopicName, groupId, bootstrap server, offset need to be provided in the order");
            System.exit(-1);
        }

        in = new Scanner(System.in);
        String topicName = args[0];
        String groupId = args[1];
        String brokerHost = args[2];
        String offset = args[3];

        ConsumerThread consumerThread = new ConsumerThread(topicName, groupId, brokerHost, offset);
        consumerThread.start();
        String line = "";
        while (!line.equalsIgnoreCase("exit")) {
            line = in.next();
        }

        consumerThread.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer");
        consumerThread.join();

    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private String brokerHost;
        private Long offset;
        private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId, String brokerHost, String offset) {
            this.topicName = topicName;
            this.groupId = groupId;
            this.brokerHost = brokerHost;
            this.offset = Long.parseLong(offset);
        }

        public void run() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,"test-offset-reset");

            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            try {
                kafkaConsumer.poll(0);
                TopicPartition topicPartition = new TopicPartition(topicName, 0);
                OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(topicPartition);
                System.out.println("current offset "+ offsetAndMetadata);
                OffsetAndMetadata newOffsetAndMetadata = new OffsetAndMetadata(offset);
                Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
                commitOffsets.put(topicPartition, newOffsetAndMetadata);
                kafkaConsumer.commitSync(commitOffsets);
                OffsetAndMetadata committedOffset = kafkaConsumer.committed(topicPartition);
                System.out.println("committed Offset"+committedOffset);
                kafkaConsumer.unsubscribe();
            }
            catch (WakeupException we) {
                System.out.println("Exception while consuming messages : " + we.getMessage());
            } catch (CommitFailedException cfe) {
                System.out.println("Exception while committing offsets: " + cfe.getMessage());
            }
            finally {
                kafkaConsumer.close();
                System.out.println("Closed KafkaConsumer");
            }
        }

        public org.apache.kafka.clients.consumer.KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}
