using System.Net;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Grpc.Net.Client;
using HelloGrpc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

public class FunctionA
{
    private readonly KeyVaultService _kv;
    private readonly ILogger<FunctionA> _log;
    private readonly KafkaProducerService _kafka;

    public FunctionA(KeyVaultService kv, KafkaProducerService kafka, ILogger<FunctionA> log)
    {
        _kv  = kv;
        _kafka = kafka;
        _log = log;
    }

    [Function("HelloHttp")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "hello")]
        HttpRequestData req)
    {
        _log.LogInformation("Function A triggered");

        // 1. Parse request body
        var body   = await req.ReadAsStringAsync() ?? "{}";
        var input  = JsonSerializer.Deserialize<OrderRequest>(body)
                     ?? new OrderRequest { Name = "Ranjit", Amount = 100 };

        // 2. Read SB connection string from Key Vault
        var sbConn = await _kv.GetSecretAsync("ServiceBusConnectionString");

        // 3. Call gRPC service
        var grpcUrl = Environment.GetEnvironmentVariable("GrpcServerUrl")
                      ?? "http://localhost:5179";
        // wrap gRPC call in try/catch so it degrades gracefully
        string grpcReply = "gRPC unavailable";
        try
        {
            grpcReply = await CallGrpcAsync(grpcUrl, input.Name, input.OrderId);
        }
        catch (Exception ex)
        {
            _log.LogWarning("gRPC call failed: {Error}", ex.Message);
        }

        // 4. Build event and send to Service Bus
        var order = new OrderEvent
        {
            OrderId     = input.OrderId,
            Name        = input.Name,
            Amount      = input.Amount,
            GrpcReply   = grpcReply,
            ProcessedAt = DateTime.UtcNow.ToString("o")
        };

        // Inside Run() — replace the SendToServiceBusAsync call with:
        await Task.WhenAll(
            SendToServiceBusAsync(sbConn, order),
            _kafka.PublishOrderAsync(order)
        );

        _log.LogInformation("Order {Id} published to Service Bus + Kafka", order.OrderId);

        // 5. Return response
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            message   = "Order accepted",
            orderId   = order.OrderId,
            grpcReply = grpcReply
        });
        return response;
    }

    private static async Task<string> CallGrpcAsync(
        string url, string name, string orderId)
    {
        using var channel = GrpcChannel.ForAddress(url);
        var client = new Greeter.GreeterClient(channel);
        var reply  = await client.SayHelloAsync(new HelloRequest
        {
            Name    = name,
            OrderId = orderId
        });
        return reply.Message;
    }

    private static async Task SendToServiceBusAsync(
        string connStr, OrderEvent order)
    {
        await using var client = new ServiceBusClient(connStr);
        var sender  = client.CreateSender("order-events");
        var payload = JsonSerializer.Serialize(order);
        await sender.SendMessageAsync(new ServiceBusMessage(payload)
        {
            ContentType = "application/json",
            Subject     = "OrderPlaced",
            MessageId   = order.OrderId
        });
    }
}

public class OrderRequest
{
    public string OrderId { get; set; } = Guid.NewGuid().ToString();
    public string Name    { get; set; } = "Ranjit";
    public double Amount  { get; set; } = 100;
}