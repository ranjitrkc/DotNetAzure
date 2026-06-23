using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Functions.Worker.SignalRService;

public class SignalRFunctions
{
    [Function("negotiate")]
    public static SignalRConnectionInfo Negotiate(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", "get")]
        HttpRequestData req,
        [SignalRConnectionInfoInput(HubName = "orders")]
        SignalRConnectionInfo connectionInfo)
    {
        // Returns SignalR endpoint + access token to the browser
        return connectionInfo;
    }
}
