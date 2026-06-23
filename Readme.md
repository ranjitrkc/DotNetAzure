# HelloAzurePoc — Azure Microservices Reference Architecture

> An event-driven microservices pipeline demonstrating 7 Azure services wired into a single traceable flow — built as a hands-on reference for Senior/Lead .NET engineering roles.

![.NET 8](https://img.shields.io/badge/.NET-8.0-512BD4?style=flat-square&logo=dotnet)
![Azure Functions](https://img.shields.io/badge/Azure_Functions-v4-0078D4?style=flat-square&logo=azure-functions)
![Service Bus](https://img.shields.io/badge/Service_Bus-Basic-0078D4?style=flat-square)
![Cosmos DB](https://img.shields.io/badge/Cosmos_DB-MongoDB_API-0078D4?style=flat-square)
![SignalR](https://img.shields.io/badge/SignalR-Serverless-0078D4?style=flat-square)

---

## What this is

A production-pattern POC demonstrating how 7 Azure services can be composed into a reliable, observable, real-time event pipeline using .NET 8 isolated worker Azure Functions.

One HTTP request triggers a chain that:

1. Reads secrets from **Azure Key Vault** via Managed Identity (zero hardcoded credentials)
2. Calls an internal **gRPC** service for order enrichment
3. Publishes to **Azure Service Bus** with PeekLock delivery guarantee
4. Persists the event document to **Azure Cosmos DB** (MongoDB API)
5. Uploads the raw payload as an immutable file to **Azure Blob Storage**
6. Tracks a custom business event in **Application Insights**
7. Pushes a real-time notification to the browser via **Azure SignalR Service**

The entire flow completes in under one second and is fully traceable end-to-end in Application Insights Transaction Search.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Azure (rg-hello-azure-poc)                   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Application Insights — traces everything         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌────────────┐    ┌─────────────────────┐    ┌─────────────┐   │
│  │ Key Vault  │◄───│    Function A        │◄───│   Client    │   │
│  │  secrets   │    │  HTTP trigger        │    │  (Postman)  │   │
│  └────────────┘    │  + gRPC call         │    └─────────────┘   │
│                    └──────────┬──────────┘                       │
│  ┌────────────┐               │ sends message                    │
│  │   gRPC     │◄──────────────┤                                  │
│  │  service   │               ▼                                  │
│  └────────────┘    ┌─────────────────────┐                       │
│                    │    Service Bus       │                       │
│                    │  queue: order-events │                       │
│                    └──────────┬──────────┘                       │
│                               │ triggers                         │
│                    ┌──────────▼──────────┐                       │
│                    │    Function B        │                       │
│                    │  SB trigger          │                       │
│                    └──┬──────┬───────┬───┘                       │
│                       │      │       │                           │
│              ┌────────▼─┐ ┌──▼────┐ ┌▼─────────┐               │
│              │ Cosmos DB│ │ Blob  │ │ SignalR  │               │
│              │  events  │ │files  │ │  push    │               │
│              └──────────┘ └───────┘ └────┬─────┘               │
│                                          │                       │
└──────────────────────────────────────────┼───────────────────────┘
                                           ▼
                                    ┌─────────────┐
                                    │   Browser   │
                                    │ live orders │
                                    └─────────────┘
```

---

## Request flow

| Step | Service | What happens |
|------|---------|-------------|
| 1 | HTTP trigger | `POST /api/hello` received by Function A |
| 2 | Key Vault | Connection string fetched via Managed Identity — no secrets in config |
| 3 | gRPC | Internal `SayHello` RPC called, enriched response returned |
| 4 | Service Bus | Order event published to `order-events` queue with PeekLock |
| 5 | Service Bus trigger | Function B fires automatically within milliseconds |
| 6 | Cosmos DB | Order document persisted to `hello-db/events` collection |
| 7 | Blob Storage | Raw JSON payload uploaded to `event-payloads/{orderId}.json` |
| 8 | App Insights | Custom `OrderProcessed` event tracked with business dimensions |
| 9 | SignalR | Real-time push to all connected browser clients |

---

## Services used

| Service | Tier | Monthly cost | Why this tier |
|---------|------|-------------|----------------|
| Azure Functions | Consumption (Windows) | Free (1M executions) | Scale to zero, pay-per-execution |
| Azure Key Vault | Standard | ~$0 for POC | Per-operation pricing, no base fee |
| Azure Service Bus | Basic | ~$0.05/million ops | Queues only — no Topics needed |
| Azure Cosmos DB | Serverless | ~$0.25/million RUs | Sporadic POC traffic — no minimum |
| Azure Blob Storage | LRS Hot | Free (5 GB / 12 months) | Standard object storage |
| Application Insights | Pay-as-you-go | Free (5 GB/month) | Full observability within free tier |
| Azure SignalR | Free tier | $0 | 20 connections, 20K msg/day |

**Total monthly cost for this POC: under $1**

---

## Key engineering decisions

These are the decisions that matter in production — each one has a reason.

### PeekLock over ReceiveAndDelete

Service Bus messages are received with `PeekLock`, not deleted on receipt. The message stays in the queue (locked, invisible to others) until `CompleteMessageAsync` is called after successful processing. If Function B crashes mid-flight, the lock expires and the message reappears for retry. After `maxDeliveryCount` failures (default 10), Service Bus automatically moves it to the dead-letter queue.

`ReceiveAndDelete` is never used in production — a process crash means the message is gone permanently.

### Managed Identity over connection strings

Function A has no Service Bus connection string in its config. Instead the Function App has a System-Assigned Managed Identity in Azure AD, and that identity is granted `Key Vault Secrets User` RBAC role. At runtime, `DefaultAzureCredential` transparently authenticates using the Managed Identity token — no password, no rotation logic, no secret sprawl.

Locally, the same `DefaultAzureCredential` falls back to the developer's `az login` session. Zero code difference between local and production.

### Lazy MongoClient initialisation

`CosmosDbService` stores the connection string in the constructor but does not call `new MongoClient()` until the first actual database operation. This prevents DNS resolution failures (common with MongoDB clusters at cold start) from crashing the Function App before any functions are registered.

```csharp
public CosmosDbService(string connectionString)
{
    _connectionString = connectionString; // store only — do not connect here
}
```

### Blob Storage as immutable audit trail

Every order event is stored twice — once as a queryable document in Cosmos DB, and once as an immutable raw JSON file in Blob Storage. The blob is the ground truth: it preserves the exact payload as received, including the gRPC reply, before any transformation. This supports event replay and audit requirements without a separate event store.

### Graceful degradation on gRPC

The gRPC call in Function A is wrapped in a try/catch. If the Greeter service is unavailable, Function A continues with `grpcReply = "gRPC unavailable"` and still sends the event to Service Bus. The order pipeline does not fail because an enrichment service is down.

```csharp
string grpcReply = "gRPC unavailable";
try { grpcReply = await CallGrpcAsync(grpcUrl, input.Name, input.OrderId); }
catch (Exception ex) { _log.LogWarning("gRPC call failed: {Error}", ex.Message); }
```

### Serverless SignalR mode

Azure SignalR Service is deployed in Serverless mode, not Classic. In Serverless mode, Azure Functions uses an output binding to send messages directly — no persistent Hub server process is needed. The `negotiate` function handles client connection setup; Function B returns a `SignalRMessageAction` as its output. Classic mode would require a running ASP.NET Core Hub, which defeats the serverless model.

---

## Project structure

```
HelloAzurePoc/
├── HelloGrpc/                        # gRPC server (.NET 8)
│   ├── Protos/greet.proto            # service contract
│   └── Services/GreeterService.cs    # SayHello implementation
│
├── HelloGrpcClient/                  # local test client
│   └── Program.cs
│
└── HelloFunctionApp/                 # Azure Functions (isolated worker)
    ├── FunctionA.cs                  # HTTP trigger — entry point
    ├── FunctionB.cs                  # Service Bus trigger — processor
    ├── SignalRNegotiate.cs           # negotiate endpoint for browser
    ├── KeyVaultService.cs            # secret reader with in-memory cache
    ├── CosmosDbService.cs            # MongoDB driver with lazy init
    ├── BlobStorageService.cs         # Azure.Storage.Blobs wrapper
    ├── Models/OrderEvent.cs          # shared message model
    ├── Protos/greet.proto            # gRPC client stub source
    ├── local.settings.json           # local config (gitignored)
    ├── host.json
    └── Program.cs                    # DI registrations
```

---

## How to run locally

### Prerequisites

- .NET 8 SDK
- Azure Functions Core Tools v4 (`npm install -g azure-functions-core-tools@4`)
- Azure CLI (`az login`)
- Active Azure subscription with the resources below provisioned

### Required Azure resources

| Resource | Name |
|----------|------|
| Resource Group | `rg-hello-azure-poc` |
| Key Vault | `kv-hello-poc` |
| Service Bus namespace | `sb-hello-poc` |
| Service Bus queue | `order-events` |
| Cosmos DB (MongoDB API) | `cosmos-hello-poc` |
| Storage Account | `sthellopocrrc` |
| Application Insights | `ai-hello-poc` |
| SignalR Service | `sr-hello-poc` (Serverless mode) |

### Key Vault secrets required

| Secret name | Value |
|-------------|-------|
| `ServiceBusConnectionString` | Service Bus primary connection string |
| `CosmosDbConnectionString` | Cosmos DB / MongoDB connection string |
| `StorageConnectionString` | Storage Account connection string |

### local.settings.json

Create `HelloFunctionApp/local.settings.json` (this file is gitignored):

```json
{
  "IsEncrypted": false,
  "Host": {
    "CORS": "http://localhost:8080",
    "CORSCredentials": false
  },
  "Values": {
    "AzureWebJobsStorage"                   : "<storage-connection-string>",
    "FUNCTIONS_WORKER_RUNTIME"              : "dotnet-isolated",
    "KeyVaultUri"                           : "<KeyVaultUri>",
    "GrpcServerUrl"                         : "http://localhost:5002",
    "ServiceBusQueueName"                   : "order-events",
    "SbConnectionString"                    : "<service-bus-connection-string>",
    "CosmosDbConnectionString"              : "<cosmos-connection-string>",
    "StorageConnectionString"               : "<storage-connection-string>",
    "AzureSignalRConnectionString"          : "<signalr-connection-string>",
    "APPLICATIONINSIGHTS_CONNECTION_STRING" : "<app-insights-connection-string>"
  }
}
```

### Start

```bash
# Terminal 1 — gRPC server
cd HelloGrpc
dotnet run

# Terminal 2 — Function App
cd HelloFunctionApp
dotnet run

# Terminal 3 — serve the browser client
cd ..
python -m http.server 8080
```

Open `http://localhost:8080/index.html` in Chrome, then send a test request:

```bash
curl -X POST http://localhost:7071/api/hello \
  -H "Content-Type: application/json" \
  -d '{"name":"Ranjit","orderId":"ORD-001","amount":2500}'
```

---

## How to deploy to Azure

```bash
cd HelloFunctionApp
dotnet publish -c Release
func azure functionapp publish func-hello-poc --dotnet-isolated
```

After deployment, add all `local.settings.json` Values as App Settings in the Function App portal, then enable System-Assigned Managed Identity and grant it `Key Vault Secrets User` role on the Key Vault.

---

## Observability

Every invocation is traced end-to-end in Application Insights. Navigate to:

```
Application Insights → Transaction search → click any request
→ full dependency chain: HTTP trigger → SB send → SB trigger → Cosmos write → Blob upload
→ duration, success/fail, and custom OrderProcessed business event per invocation
```

Custom KQL query to view business metrics:

```kusto
customEvents
| where name == "OrderProcessed"
| extend orderId = tostring(customDimensions.OrderId)
| extend amount  = todouble(customDimensions.Amount)
| summarize count(), avg(amount) by bin(timestamp, 1h)
| order by timestamp desc
```

---

## Patterns demonstrated

| Pattern | Where | Why it matters |
|---------|-------|---------------|
| PeekLock + DLQ | FunctionB.cs | Message survives consumer crash |
| Managed Identity | KeyVaultService.cs | Zero secret rotation overhead |
| Lazy initialisation | CosmosDbService.cs | Avoids startup failure on cold DB |
| Graceful degradation | FunctionA.cs | Pipeline continues if gRPC is down |
| Immutable audit trail | BlobStorageService.cs | Raw event preserved for replay |
| Custom telemetry | FunctionB.cs | Business metrics in App Insights |
| Serverless SignalR | SignalRNegotiate.cs | Real-time push without Hub server |
| DI singleton services | Program.cs | One MongoClient shared per instance |

---

## Tech stack

```
Language    : C# / .NET 8
Runtime     : Azure Functions v4 — isolated worker model
Messaging   : Azure Service Bus (Basic tier, PeekLock)
Database    : Azure Cosmos DB for MongoDB — MongoDB.Driver NuGet
Storage     : Azure Blob Storage — Azure.Storage.Blobs NuGet
Secrets     : Azure Key Vault — Azure.Identity DefaultAzureCredential
RPC         : gRPC — Grpc.Net.Client, Google.Protobuf, Grpc.Tools
Real-time   : Azure SignalR Service (Serverless mode)
Observability: Application Insights — custom events + dependency tracking
IaC         : Manual portal provisioning (ARM/Bicep templates planned)
```

---

## Roadmap

- [ ] Idempotency check — skip reprocessing duplicate `orderId` messages
- [ ] Dead-letter queue monitor function — alert on DLQ messages
- [ ] Health check endpoint — verify all downstream dependencies
- [ ] Outbox pattern — guaranteed SB delivery even during transient failures
- [ ] Bicep templates — one-command environment provisioning
- [ ] GitHub Actions CI/CD — automated deploy on push to main

---

## Author

**Ranjit Chandrasekaran** — Tech Lead & Full-Stack .NET Engineer  
10+ years across ERP, logistics, insurance, and fintech domains  
Core stack: C# / .NET Core · Azure · Kafka · MongoDB · gRPC · microservices

[LinkedIn](https://linkedin.com/in/your-profile) · [GitHub](https://github.com/your-username)