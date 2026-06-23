using Confluent.Kafka;
using System.Text.Json;
using Microsoft.Extensions.Logging;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string>      _producer;
    private readonly string                          _topic;
    private readonly ILogger<KafkaProducerService>  _log;

    public KafkaProducerService(
        string bootstrapServers,
        string topic,
        ILogger<KafkaProducerService> log)
    {
        _topic = topic;
        _log   = log;

        var config = new ProducerConfig
        {
            BootstrapServers      = bootstrapServers,
            Acks                  = Acks.All,
            EnableIdempotence     = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs        = 1000,
            LingerMs              = 5,
            CompressionType       = CompressionType.Snappy
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishOrderAsync(OrderEvent order)
    {
        var payload = JsonSerializer.Serialize(order);

        var message = new Message<string, string>
        {
            Key   = order.OrderId,
            Value = payload,
            Headers = new Headers
            {
                { "source",    System.Text.Encoding.UTF8.GetBytes("HelloFunctionApp") },
                { "eventType", System.Text.Encoding.UTF8.GetBytes("OrderPlaced")      }
            }
        };

        try
        {
            var result = await _producer.ProduceAsync(_topic, message);
            _log.LogInformation(
                "Kafka published — Topic: {Topic} | Partition: {P} | Offset: {O} | OrderId: {Id}",
                result.Topic, result.Partition.Value, result.Offset.Value, order.OrderId);
        }
        catch (ProduceException<string, string> ex)
        {
            _log.LogError(ex, "Kafka publish failed — {Error}", ex.Error.Reason);
            throw;
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}