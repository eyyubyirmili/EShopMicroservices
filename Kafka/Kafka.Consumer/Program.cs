// See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka, Consumer!");

var kafkaService = new KafkaService();
//var topicName = "use-case-4-topic";
var topicName = "mycluster2-topic";



//await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);
//await kafkaService.ConsumeSimpleMessageWithIntKey(topicName);
//await kafkaService.ConsumeComplexMessageWithIntKey(topicName);
//await kafkaService.ConsumeComplexMessageWithComplexKey(topicName);
//await kafkaService.ConsumeComplexMessageWitfTimestamp(topicName);
//await kafkaService.ConsomerMessageFromSpecificPartition(topicName);
await kafkaService.ConsomerMessageFromCluster(topicName);





Console.ReadLine();