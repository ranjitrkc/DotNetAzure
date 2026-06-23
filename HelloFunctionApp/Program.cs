using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((ctx, services) =>
    {
        var vaultUri = ctx.Configuration["KeyVaultUri"]
                    ?? "https://kv-hello-poc.vault.azure.net/";
        services.AddSingleton(new KeyVaultService(vaultUri));

        var cosmosConn = ctx.Configuration["CosmosDbConnectionString"]
                      ?? string.Empty;
        services.AddSingleton(new CosmosDbService(cosmosConn));

        var storageConn = ctx.Configuration["StorageConnectionString"]
                       ?? string.Empty;
        services.AddSingleton(new BlobStorageService(storageConn));

        var kafkaServers = ctx.Configuration["KafkaBootstrapServers"] ?? "localhost:9092";
        var kafkaTopic   = ctx.Configuration["KafkaOrdersTopic"] ?? "order-events-kafka";

        services.AddSingleton(sp =>
            new KafkaProducerService(
                kafkaServers,
                kafkaTopic,
                sp.GetRequiredService<ILogger<KafkaProducerService>>()));

        // App Insights — just this one line, no ConfigureFunctionsApplicationInsights
        services.AddApplicationInsightsTelemetryWorkerService();
    })
    .Build();

host.Run();