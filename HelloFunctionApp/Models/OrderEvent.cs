using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

public class OrderEvent
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }           // MongoDB _id — auto generated
    public string OrderId     { get; set; } = Guid.NewGuid().ToString();
    public string Name        { get; set; } = string.Empty;
    public double Amount      { get; set; }
    public string GrpcReply   { get; set; } = string.Empty;
    public string ProcessedAt { get; set; } = DateTime.UtcNow.ToString("o");
}