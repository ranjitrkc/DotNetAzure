using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

public class FunctionDlq
{
    private readonly TelemetryClient      _telemetry;
    private readonly ILogger<FunctionDlq> _log;

    public FunctionDlq(TelemetryClient telemetry, ILogger<FunctionDlq> log)
    {
        _telemetry = telemetry;
        _log       = log;
    }

    [Function("DlqMonitor")]
    public async Task Run(
        [ServiceBusTrigger(
            "order-events/$deadletterqueue",
            Connection = "SbConnectionString")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions)
    {
        var reason      = message.DeadLetterReason ?? "Unknown";
        var description = message.DeadLetterErrorDescription ?? "No description";
        var body        = message.Body.ToString();

        _log.LogError(
            "DLQ message — MessageId: {Id} | Reason: {Reason} | " +
            "Description: {Desc} | DeliveryCount: {Count}",
            message.MessageId, reason, description, message.DeliveryCount);

        // Track in App Insights — queryable via KQL
        _telemetry.TrackEvent("OrderDeadLettered",
            new Dictionary<string, string>
            {
                { "MessageId",     message.MessageId                },
                { "Reason",        reason                           },
                { "Description",   description                      },
                { "DeliveryCount", message.DeliveryCount.ToString() },
                { "EnqueuedTime",  message.EnqueuedTime.ToString("o") },
                { "Body",          body.Length > 500 ? body[..500] + "..." : body }
            });

        // Try to extract orderId for correlation
        try
        {
            var order = JsonSerializer.Deserialize<OrderEvent>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (order != null)
                _log.LogError("Dead-lettered order — OrderId: {Id}, Name: {Name}",
                    order.OrderId, order.Name);
        }
        catch
        {
            _log.LogWarning("Could not deserialize DLQ message body");
        }

        // Complete removes it from DLQ
        // In production: write to failed-orders store or trigger alert here
        await actions.CompleteMessageAsync(message);
        _log.LogInformation("DLQ message {Id} acknowledged", message.MessageId);
    }
}