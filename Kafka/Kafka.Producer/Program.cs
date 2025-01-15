// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka, Producer!");

var kafkaService = new KafkaService();
//var topicName = "use-case-1.1-topic";
//var topicName = "use-case-4-topic";
//var topicName = "ack-retention-topic";
//var topicName = "mycluster2-topic";
var topicName = "retry-topic";


//await kafkaService.CreateTopicAsync(topicName);
//await kafkaService.SendSimpleMessageWithNullKey(topicName);
//await kafkaService.SendSimpleMessageWithIntKey(topicName);
//await kafkaService.SendComplexMessageWithIntKey(topicName);
//await kafkaService.SendComplexMessageWithComplexKey(topicName);
//await kafkaService.SendMessageWithTimeStamp(topicName);
//await kafkaService.SendMessagToSpecificPartition(topicName);
//await kafkaService.SendMessagWithAck(topicName);
//await kafkaService.CreateTopicWithRetentionAsync(topicName);
//await kafkaService.CreateTopicWithClusterAsync(topicName);
//await kafkaService.SendMessagToClusterAsync(topicName);
//await kafkaService.CreateTopicRetryWithClusterAsync(topicName);
await kafkaService.SendMessagWithRetryToClusterAsync(topicName);












Console.WriteLine("Messages Sended");

