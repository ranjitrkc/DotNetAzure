using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

public class KeyVaultService
{
    private readonly SecretClient _client;
    private readonly Dictionary<string, string> _cache = new();

    public KeyVaultService(string vaultUri)
    {
        _client = new SecretClient(
            new Uri(vaultUri),
            new DefaultAzureCredential());
    }

    public async Task<string> GetSecretAsync(string name)
    {
        if (_cache.TryGetValue(name, out var cached))
            return cached;

        var s = await _client.GetSecretAsync(name);
        _cache[name] = s.Value.Value;
        return s.Value.Value;
    }
}