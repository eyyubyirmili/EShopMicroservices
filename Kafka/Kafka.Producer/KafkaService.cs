using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Producer.Events;
using System.Globalization;
using System.Diagnostics;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        //private const string TopicName = "topic_one";

        internal async Task CreateTopicAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                //// https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
                //var config = new Dictionary<string, string>()
                //{
                //    {"message.timestamp.type", "LogAppendTime" }
                //};

                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName,
                        NumPartitions = 6,
                        ReplicationFactor = 1
                        //Configs = config
                    }
                });

                Console.WriteLine($"Topic({topicName}) Created");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
        }

        internal async Task SendSimpleMessageWithNullKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var messageData = new Message<Null, string>()
                {
                    Value = $"Message(use-case-1) - {item}"
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendSimpleMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var messageData = new Message<int, string>()
                {
                    Value = $"Message(use-case-1) - {item}",
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendComplexMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var orderCreatedEven = new OrderCreatedEvent() { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };


                var messageData = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEven,
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();

            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEven = new OrderCreatedEvent() { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };

                var header = new Headers
                {
                    { "correlation_id", "123"u8.ToArray() },
                    { "version", Encoding.UTF8.GetBytes("v1") }
                };

                var messageData = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEven,
                    Key = item,
                    Headers = header
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendComplexMessageWithComplexKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();

            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEven = new OrderCreatedEvent() { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };

                var messageData = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEven,
                    Key = new MessageKey("key" + item, "key" + (item + 1))
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendMessageWithTimeStamp(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();

            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEven = new OrderCreatedEvent() { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };

                var messageData = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEven,
                    Key = new MessageKey("key" + item, "key" + (item + 1)),
                    Timestamp = new Timestamp(new DateTime(2024, 12, 22))
                };

                var result = await producer.ProduceAsync(topicName, messageData);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendMessagToSpecificPartition(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var message = new Message<Null, string>()
                {
                    Value = $"Message: {item}"
                };

                //Data direk Partition 2 ye gönderilir 
                var topicPartition = new TopicPartition(topicName, new Partition(2));

                var result = await producer.ProduceAsync(topicPartition, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task SendMessagWithAck(string topicName)
        {
            //var config = new ProducerConfig() { BootstrapServers = "localhost:9094" , Acks = Acks.None};
            //var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.Leader };
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.All };


            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var message = new Message<Null, string>()
                {
                    Value = $"Message: {item}"
                };

                //Data direk Partition 2 ye gönderilir 
                var topicPartition = new TopicPartition(topicName, new Partition(2));

                var result = await producer.ProduceAsync(topicPartition, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task CreateTopicWithRetentionAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                //// https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#retention-ms
                ///
                TimeSpan day30Span = TimeSpan.FromDays(30);
                var config = new Dictionary<string, string>()
                {
                    //{"retention.bytes","10000"} Topic partition byte kısıtı 10kb
                    //{"retention.ms", "-1" } Ömüt boyu kalır 
                    {"retention.ms", day30Span.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) } // Topic partition süre kısıtı
                };



                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName,
                        NumPartitions = 6,
                        ReplicationFactor = 1,
                        Configs = config
                    }
                });

                Console.WriteLine($"Topic({topicName}) Created");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
        }

        internal async Task CreateTopicWithClusterAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"
            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName,
                        NumPartitions = 6,
                        ReplicationFactor = 3
                    }
                });

                Console.WriteLine($"Topic({topicName}) Created");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
        }

        internal async Task SendMessagToClusterAsync(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All };


            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var message = new Message<Null, string>()
                {
                    Value = $"Message: {item}"
                };

                //Data direk Partition 2 ye gönderilir 
                var topicPartition = new TopicPartition(topicName, new Partition(2));

                var result = await producer.ProduceAsync(topicPartition, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------------");
                await Task.Delay(200);
            }


        }

        internal async Task CreateTopicRetryWithClusterAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"
            }).Build();

            try
            {
                //https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
                var config = new Dictionary<string, string>()
                {
                    { "min.insync.replicas", "3" }
                };



                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName,
                        NumPartitions = 6,
                        ReplicationFactor = 3,
                        Configs = config
                    }
                });

                Console.WriteLine($"Topic({topicName}) Created");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
        }

        internal async Task SendMessagWithRetryToClusterAsync(string topicName)
        {
            //Task.Run(async () =>
            //{
            //    Stopwatch stopwatch = Stopwatch.StartNew();
            //    stopwatch.Start();
            //    while (true)
            //    {
            //        TimeSpan timeSpan = TimeSpan.FromSeconds(Convert.ToInt32(stopwatch.Elapsed.TotalSeconds));
            //        Console.Write(timeSpan.ToString("c"));
            //        Console.Write(timeSpan.ToString("\r"));


            //        await Task.Delay(1000);
            //    }

            //});


            var config = new ProducerConfig() { 
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", 
                Acks = Acks.All,
                MessageTimeoutMs = 5000,
                RetryBackoffMs = 2000,
                ReconnectBackoffMaxMs = 2000
                //MessageSendMaxRetries = 5
            };

            


            using var producer = new ProducerBuilder<Null, string>(config).Build();



            var message = new Message<Null, string>()
            {
                Value = $"Message: 1"
            };

            //Data direk Partition 2 ye gönderilir 
            var topicPartition = new TopicPartition(topicName, new Partition(2));

            var result = await producer.ProduceAsync(topicPartition, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("-----------------------------------------");
            await Task.Delay(200);



        }
    }
}
