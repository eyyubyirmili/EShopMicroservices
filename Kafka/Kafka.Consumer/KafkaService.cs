using Confluent.Kafka;
using Kafka.Consumer.Events;
using System.Text;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig() 
            { 
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null,string>(config).Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if(consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj {consumeResult.Message.Value}");
                }

                await Task.Delay(500);
            }

        }

        internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case--group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj : Key {consumeResult.Message.Key} Value= {consumeResult.Message.Value}");
                }

                await Task.Delay(500);
            }

        }

        internal async Task ConsumeComplexMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-3-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"gelen mesaj :  {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }

        }

        internal async Task ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-3-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));
                    var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                    Console.WriteLine($"Headers: {correlationId} - {version}");

                    //var correlationId = consumeResult.Message.Headers[0].GetValueBytes();
                    //var version = consumeResult.Message.Headers[1].GetValueBytes();



                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"gelen mesaj :  {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }

        }

        internal async Task ConsumeComplexMessageWithComplexKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-4-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
            .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
            .Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;

                    Console.WriteLine($"gelen mesaj(key) => key1:{messageKey.key1} - key2:{messageKey.key2}");


                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"gelen mesaj :  {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }

        }

        internal async Task ConsumeComplexMessageWitfTimestamp(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-5-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
            .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
            .Build();
            consumer.Subscribe(topicName);


            //Metod Kafkada bulunan mesajları Memoriye alıp tek tek işler mesaj bitince tekrar nekadar var ise alır ve aynı süreç devam eder 
            // Topik te mesaj yok ise Consume metodu While döngüsü içerinde bloklanır. 
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp : {consumeResult.Message.Timestamp.UtcDateTime}");
                }
                //var _date = new DateTimeOffset()
                await Task.Delay(10);
            }

        }

        internal async Task ConsomerMessageFromSpecificPartition(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-5-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();

            //ConsomerMessageFromSpecificPartition
            //consumer.Assign(new TopicPartition(topicName,new Partition(2)));

            //ConsomerMessageFromSpecificPartitionOfset
            //consumer.Assign(new List<TopicPartition>() {new TopicPartition(topicName, 0) });

            consumer.Assign(new TopicPartitionOffset(topicName, 2, 4));

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");
                }
                //var _date = new DateTimeOffset()
                await Task.Delay(10);
            }

        }

        internal async Task ConsomerMessageWithAct(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "group-4",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                //EnableAutoCommit = true
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    try
                    {
                        Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");

                        consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                    
                }
                //var _date = new DateTimeOffset()
                await Task.Delay(10);
            }

        }

        internal async Task ConsomerMessageFromCluster(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                GroupId = "group-x",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                //EnableAutoCommit = true
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    try
                    {
                        Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");

                        consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }

                }
                //var _date = new DateTimeOffset()
                await Task.Delay(10);
            }

        }
    }
}
