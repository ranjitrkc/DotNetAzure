using Grpc.Core;

namespace HelloGrpc.Services;

public class GreeterService : Greeter.GreeterBase
{
    private readonly ILogger<GreeterService> _logger;

    public GreeterService(ILogger<GreeterService> logger)
    {
        _logger = logger;
    }

    public override Task<HelloReply> SayHello(
        HelloRequest request,
        ServerCallContext context)
    {
        _logger.LogInformation(
            "gRPC SayHello called — name: {Name}, orderId: {OrderId}",
            request.Name, request.OrderId);

        return Task.FromResult(new HelloReply
        {
            Message     = $"Hello {request.Name}! Order {request.OrderId} received.",
            ProcessedAt = DateTime.UtcNow.ToString("o"),
            GrpcServer  = "HelloGrpc-v1"
        });
    }
}