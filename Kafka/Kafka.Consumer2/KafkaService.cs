using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Consumer2
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig() 
            { 
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-2",
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
    }
}
