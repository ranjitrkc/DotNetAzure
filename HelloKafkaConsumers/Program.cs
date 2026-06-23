using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrapServers = "localhost:9092";
const string topic            = "order-events-kafka";

Console.WriteLine("Starting Kafka consumers...");

// Create topic if it doesn't exist
try
{
    using var adminClient = new AdminClientBuilder(
        new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

    await adminClient.CreateTopicsAsync(new[]
    {
        new TopicSpecification
        {
            Name              = topic,
            NumPartitions     = 3,
            ReplicationFactor = 1
        }
    });
    Console.WriteLine($"Topic '{topic}' created.");
}
catch (CreateTopicsException e)
    when (e.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
{
    Console.WriteLine($"Topic '{topic}' already exists — continuing.");
}

await Task.WhenAll(
    RunConsumerAsync("order-audit-group",     "Audit    "),
    RunConsumerAsync("order-analytics-group", "Analytics")
);

static async Task RunConsumerAsync(string groupId, string label)
{
    var config = new ConsumerConfig
    {
        BootstrapServers    = bootstrapServers,
        GroupId             = groupId,
        AutoOffsetReset     = AutoOffsetReset.Earliest,
        EnableAutoCommit    = false,
        SessionTimeoutMs    = 10000,
        HeartbeatIntervalMs = 3000
    };

    using var consumer = new ConsumerBuilder<string, string>(config)
        .SetErrorHandler((_, e) =>
            Console.WriteLine($"[{label}] Error: {e.Reason}"))
        .SetPartitionsAssignedHandler((_, partitions) =>
            Console.WriteLine($"[{label}] Assigned: " +
                string.Join(", ", partitions)))
        .Build();

    consumer.Subscribe(topic);
    Console.WriteLine($"[{label}] Subscribed → {topic} | Group: {groupId}");

    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

    try
    {
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(cts.Token);
                if (result == null) continue;

                var preview = result.Message.Value.Length > 80
                    ? result.Message.Value[..80] + "..."
                    : result.Message.Value;

                Console.WriteLine(
                    $"[{label}] Partition: {result.Partition.Value} | " +
                    $"Offset: {result.Offset.Value} | " +
                    $"Key: {result.Message.Key} | " +
                    $"Value: {preview}");

                consumer.Commit(result);
                Console.WriteLine($"[{label}] Offset {result.Offset.Value} committed");

                await Task.Delay(100);
            }
            catch (ConsumeException ex)
                when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                Console.WriteLine($"[{label}] Topic not ready yet — retrying in 2s...");
                await Task.Delay(2000);
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"[{label}] Shutting down...");
    }
    finally
    {
        consumer.Close();
    }
}