using Confluent.Kafka;
using System;

class KafkaConsumer
{
    private const string KafkaBroker = "localhost:9092";
    private const string KafkaTopic = "durable-function-output";

    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "my-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(KafkaTopic);

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Message received: {consumeResult.Message.Value}");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error: {e.Error.Reason}");
        }
    }
}

