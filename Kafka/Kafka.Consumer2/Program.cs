// See https://aka.ms/new-console-template for more information
using Kafka.Consumer2;

Console.WriteLine("Kafka, Consumer!");

var kafkaService = new KafkaService();
var topicName = "use-case-1.1-topic";

await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);

Console.ReadLine();