import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import java.util.*;

public class KafkaConsumer {
    private static Scanner in;

    public static void main(String[] args) throws InterruptedException {
        if(args.length != 5) {
            System.out.println("TopicName, groupId, bootstrap server, offset need to be provided in the order");
            System.exit(-1);
        }

        in = new Scanner(System.in);
        String topicName = args[0];
        String groupId = args[1];
        String brokerHost = args[2];
        String offset = args[3];
        String partition = args[4];

        ConsumerThread consumerThread = new ConsumerThread(topicName, groupId, brokerHost, offset, partition);
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
        private Integer partition;
        private volatile boolean commitInProgress;
        private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId, String brokerHost, String offset, String partition) {
            this.topicName = topicName;
            this.groupId = groupId;
            this.brokerHost = brokerHost;
            this.offset = Long.parseLong(offset);
            this.partition = Integer.parseInt(partition);
        }

        public void run() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,"test-offset-reset");
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            kafkaConsumer.assign(Arrays.asList(topicPartition));
            try {

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(500);
                    // check if there is something to commit
                    if (!commitInProgress) {
                        commitInProgress = true;
                        OffsetAndMetadata newOffsetAndMetadata = new OffsetAndMetadata(offset);
                        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
                        commitOffsets.put(topicPartition, newOffsetAndMetadata);
                        kafkaConsumer.commitAsync(commitOffsets, new OffsetCommitCallback() {
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
                                    commitInProgress = false;
                                    if (ex != null) {
                                        System.out.println("Auto-commit of offsets");
                                        ex.printStackTrace();
                                    } else {
                                        System.out.println("Completed auto-commit of offsets {} for group {}");
                                    }
                                }
                        });
                    }


                }
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
