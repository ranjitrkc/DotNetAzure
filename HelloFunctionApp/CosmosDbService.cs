using MongoDB.Driver;

public class CosmosDbService
{
    private readonly string _connectionString;
    private IMongoCollection<OrderEvent>? _collection;

    public CosmosDbService(string connectionString)
    {
        // Store only — do NOT connect here
        _connectionString = connectionString;
    }

    private IMongoCollection<OrderEvent> GetCollection()
    {
        if (_collection != null) return _collection;

        var settings = MongoClientSettings.FromConnectionString(_connectionString);
        settings.ServerSelectionTimeout = TimeSpan.FromSeconds(10);
        settings.ConnectTimeout = TimeSpan.FromSeconds(10);

        var client   = new MongoClient(settings);
        var database = client.GetDatabase("hello-db");
        _collection  = database.GetCollection<OrderEvent>("events");
        return _collection;
    }

    public async Task InsertOrderAsync(OrderEvent order)
    {
        await GetCollection().InsertOneAsync(order);
    }

    public async Task<List<OrderEvent>> GetAllOrdersAsync()
    {
        return await GetCollection().Find(_ => true).ToListAsync();
    }
}