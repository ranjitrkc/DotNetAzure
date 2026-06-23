using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

public class BlobStorageService
{
    private readonly BlobContainerClient _container;

    public BlobStorageService(string connectionString)
    {
        var serviceClient = new BlobServiceClient(connectionString);
        _container = serviceClient.GetBlobContainerClient("event-payloads");
    }

    public async Task UploadOrderAsync(string orderId, string jsonPayload)
    {
        // blob name = orderId.json e.g. ORD-001.json
        var blobName   = $"{orderId}.json";
        var blobClient = _container.GetBlobClient(blobName);

        var blobData = BinaryData.FromString(jsonPayload);

        await blobClient.UploadAsync(blobData, new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/json"
            }
        });
    }

    public async Task<string> DownloadOrderAsync(string orderId)
    {
        var blobClient = _container.GetBlobClient($"{orderId}.json");
        var response   = await blobClient.DownloadContentAsync();
        return response.Value.Content.ToString();
    }
}