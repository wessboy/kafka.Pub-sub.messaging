using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafkaConsumer
{
     public class ConsumeMessage
    {
        public void ReadMessage()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:2282",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ClientId = "my-app",
                GroupId = "my-group",
                BrokerAddressFamily = BrokerAddressFamily.V4,
            };

            using var consumer = new ConsumerBuilder<Ignore,string>(config).Build();
            consumer.Subscribe("my-topic");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"Message received from {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {

              
            }
            finally { consumer.Close(); }

            Console.ReadLine();
        }
    }
}
