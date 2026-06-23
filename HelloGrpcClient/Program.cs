using Grpc.Net.Client;
using HelloGrpc;

// Point to the running gRPC server (HTTP, no TLS for local dev)
using var channel = GrpcChannel.ForAddress("http://localhost:5179");

var client = new Greeter.GreeterClient(channel);

var reply = await client.SayHelloAsync(new HelloRequest
{
    Name    = "Ranjit",
    OrderId = "ORD-001"
});

Console.WriteLine($"Response  : {reply.Message}");
Console.WriteLine($"Processed : {reply.ProcessedAt}");
Console.WriteLine($"Server    : {reply.GrpcServer}");