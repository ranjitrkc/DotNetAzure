using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Functions.Worker.SignalRService;
using Microsoft.Extensions.Logging;

public class FunctionB
{
    private readonly CosmosDbService    _cosmos;
    private readonly BlobStorageService _blob;
    private readonly TelemetryClient    _telemetry;
    private readonly ILogger<FunctionB> _log;

    public FunctionB(
        CosmosDbService cosmos,
        BlobStorageService blob,
        TelemetryClient telemetry,
        ILogger<FunctionB> log)
    {
        _cosmos    = cosmos;
        _blob      = blob;
        _telemetry = telemetry;
        _log       = log;
    }

    [Function("ProcessOrder")]
    [SignalROutput(HubName = "orders",
                  ConnectionStringSetting = "AzureSignalRConnectionString")]
    public async Task<SignalRMessageAction> Run(
        [ServiceBusTrigger(
            "order-events",
            Connection = "SbConnectionString")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions)
    {
        _log.LogInformation("Function B triggered — MessageId: {Id}",
            message.MessageId);

        try
        {
            var json  = message.Body.ToString();
            var order = JsonSerializer.Deserialize<OrderEvent>(json,
                new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

            if (order == null)
                throw new Exception("Failed to deserialize order");

            // Phase 5 — Cosmos DB
            await _cosmos.InsertOrderAsync(order);
            _log.LogInformation("Order {Id} saved to Cosmos DB", order.OrderId);

            // Phase 6 — Blob Storage
            await _blob.UploadOrderAsync(order.OrderId, json);
            _log.LogInformation("Order {Id} uploaded to Blob Storage", order.OrderId);

            // Phase 7a — App Insights custom event
            _telemetry.TrackEvent("OrderProcessed",
                new Dictionary<string, string>
                {
                    { "OrderId", order.OrderId },
                    { "Name",    order.Name    },
                    { "Amount",  order.Amount.ToString() }
                });
            _log.LogInformation("App Insights event tracked — {Id}", order.OrderId);

            await actions.CompleteMessageAsync(message);
            _log.LogInformation("Order {Id} completed", order.OrderId);

            // Phase 7b — SignalR push to browser
            return new SignalRMessageAction("orderProcessed")
            {
                Arguments = new object[]
                {
                    new {
                        order.OrderId,
                        order.Name,
                        order.Amount,
                        order.GrpcReply,
                        order.ProcessedAt,
                        Message = "Your order has been processed!"
                    }
                }
            };
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Failed processing message {Id}",
                message.MessageId);
            throw;
        }
    }
}