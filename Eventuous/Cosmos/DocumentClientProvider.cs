using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace Common.Eventuous.Cosmos;

public interface IDocumentClientProvider
{
    CosmosClient GetDocumentClient(IOptions<StorageServiceOptions> options);
}

public class DocumentClientProvider : IDocumentClientProvider
{
    private static readonly string LocalEndpoint = "https://linux-agent:8081";
    private static readonly string LocalKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private static readonly object Lock = new object();

    private static CosmosClient? _documentClient;

    public CosmosClient GetDocumentClient(IOptions<StorageServiceOptions> storageOptions)
    {
        if (_documentClient == null)
            lock (Lock)
                if (_documentClient == null)
                {
                    var endpoint = storageOptions.Value.Endpoint ?? LocalEndpoint;
                    var key = storageOptions.Value.Key ?? LocalKey;
                    
                    CosmosClientOptions options = new ()
                    {
                        HttpClientFactory = () => new HttpClient(new HttpClientHandler
                        {
                            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                        }),
                        ConnectionMode = ConnectionMode.Gateway,
                        RequestTimeout = TimeSpan.FromMinutes(3),
                        CosmosClientTelemetryOptions = new CosmosClientTelemetryOptions
                        {
                          DisableDistributedTracing  = false,
                          QueryTextMode = QueryTextMode.All
                        },
                        UseSystemTextJsonSerializerWithOptions = new JsonSerializerOptions
                        {
                            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                        }
                    };
                    
                    _documentClient = new CosmosClient(endpoint, key, options);
                }

        return _documentClient;
    }
}