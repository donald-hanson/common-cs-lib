using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Common.Eventuous.Cosmos;

[JsonObject]
public interface IDocumentEntity
{
    string Id { get; set; }
    string DomainName { get; set; }
    string TypeName { get; set; }
    string? TenantKey { get; set; }
    ulong? GlobalPosition { get; set; }
    DateTimeOffset CreatedAtUtc { get; set; }
    DateTimeOffset UpdatedAtUtc { get; set; }
}

public interface IStorageService<T>
{
    Task<T?> GetAsync(string tenantKey, string id, CancellationToken cancellationToken = default);
    Task<T?> FirstOrDefaultAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null);
    Task<bool> AnyAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null);
    Task<IQueryable<T>> Queryable(string tenantKey);
    Task<List<T>> ExecuteQueryableAsync(IQueryable<T> queryable);
    Task<List<TSelect>> ExecuteQueryableAsync<TSelect>(IQueryable<TSelect> queryable);
    Task<List<T>> ListAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null);
    Task CreateAsync(string tenantKey, T item, CancellationToken cancellationToken = default);
    Task UpdateAsync(string tenantKey, string id, T item, CancellationToken cancellationToken = default);
    Task UpsertAsync(string tenantKey, T item, CancellationToken cancellationToken = default);
    Task UpsertAsync(string tenantKey, IEnumerable<T> items, CancellationToken cancellationToken = default);
    Task DeleteAsync(string tenantKey, string id, CancellationToken cancellationToken = default);
}

public class StorageService<T> : IStorageService<T> where T : class, IDocumentEntity
{
    private const int _throughput = 1000;

    private readonly CosmosClient _client;
    private readonly IOptions<StorageServiceOptions> _options;
    private readonly ILogger<StorageService<T>> _logger;

    private bool _initialized = false;
    private static readonly SemaphoreSlim InitSemaphore = new SemaphoreSlim(1, 1);

    public StorageService(IDocumentClientProvider clientProvider, IOptions<StorageServiceOptions> options, ILogger<StorageService<T>> logger)
    {
        ValidateGenericTypeIsSealed();
        
        _options = options;
        _logger = logger;
        _client = clientProvider.GetDocumentClient(options);
    }

    private string DatabaseId => _options.Value.DatabaseName ?? "Default";
    private string CollectionId => _options.Value.CollectionName ?? "Default";
    private string DomainName => _options.Value.DomainName ?? "Default";
    
    private PartitionKey GetPartitionKey(string tenantKey)
    {
        return new PartitionKeyBuilder()
            .Add(DomainName)
            .Add(typeof(T).Name)
            .Add(tenantKey)
            .Build();
    }

    private async Task Initialize()
    {
        await InitSemaphore.WaitAsync();
        try
        {
            if (!_initialized)
            {
                await CreateDatabaseIfNotExistsAsync();
                await CreateCollectionIfNotExistsAsync();
                _initialized = true;
            }
        }
        finally
        {
            InitSemaphore.Release();
        }
    }

    public async Task<T?> GetAsync(string tenantKey, string id, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogTrace("Get<{0}>({1},{2}) Started", typeof(T).Name, tenantKey, id);
            
            await Initialize();

            var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
            var response = await container.ReadItemAsync<T>(id, GetPartitionKey(tenantKey), cancellationToken: cancellationToken);
            return response.Resource;
        }
        catch (CosmosException e)
        {
            if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }

            throw;
        }
        finally
        {
            _logger.LogTrace("Get<{0}>({1},{2}) Finished", typeof(T).Name, tenantKey, id);
        }
    }
    
    public async Task<IEnumerable<T>> QueryAsync(string tenantKey, QueryDefinition queryDefinition)
    {
        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        var options = new QueryRequestOptions
        {
            PartitionKey = GetPartitionKey(tenantKey)
        };

        var q = container.GetItemQueryIterator<T>(queryDefinition, null, options);

        var results = new List<T>();
        while (q.HasMoreResults)
        {
            var response = await q.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }

    public async Task<bool> AnyAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null)
    {
        var result = await FirstOrDefaultAsync(tenantKey, predicate);
        return result != default(T);
    }

    public async Task<IQueryable<T>> Queryable(string tenantKey)
    {
        await Initialize();
        
        return CreateQueryable(tenantKey);
    }

    public async Task<List<T>> ExecuteQueryableAsync(IQueryable<T> queryable)
    {
        await Initialize();

        var documentQuery = queryable.ToFeedIterator();
        
        List<T> results = new List<T>();
        while (documentQuery.HasMoreResults)
        {
            var response = await documentQuery.ReadNextAsync();
            results.AddRange(response);
        }

        return results;        
    }
    
    public async Task<List<TSelect>> ExecuteQueryableAsync<TSelect>(IQueryable<TSelect> queryable)
    {
        await Initialize();

        var documentQuery = queryable.ToFeedIterator();
        
        List<TSelect> results = new List<TSelect>();
        while (documentQuery.HasMoreResults)
        {
            var response = await documentQuery.ReadNextAsync();
            results.AddRange(response);
        }

        return results;        
    }    

    public async Task<List<T>> ListAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null)
    {
        await Initialize();

        var documentQuery = CreateListQuery(tenantKey, -1, predicate);

        List<T> results = new List<T>();
        while (documentQuery.HasMoreResults)
        {
            var response = await documentQuery.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }

    public async Task<T?> FirstOrDefaultAsync(string tenantKey, Expression<Func<T, bool>>? predicate = null)
    {
        await Initialize();

        var documentQuery = CreateListQuery(tenantKey, 1, predicate);
        if (documentQuery.HasMoreResults)
        {
            var response = await documentQuery.ReadNextAsync();
            return response.Resource.FirstOrDefault();
        }
        return null;
    }

    private FeedIterator<T> CreateListQuery(string tenantKey, int maxItems = -1, Expression<Func<T, bool>>? predicate = null)
    {
        var query = CreateQueryable(tenantKey, maxItems);
        
        if (predicate != null)
            query = query.Where(predicate);

        return query.ToFeedIterator();
    }

    private IQueryable<T> CreateQueryable(string tenantKey, int maxItems = -1)
    {
        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        var options = new QueryRequestOptions
        {
            PartitionKey = GetPartitionKey(tenantKey)
        };
        if (maxItems > 0)
            options.MaxItemCount = maxItems;

        IQueryable<T> query = container.GetItemLinqQueryable<T>(false, null, options);
        return query;
    }

    public async Task CreateAsync(string tenantKey, T item, CancellationToken cancellationToken = default)
    {
        _logger.LogTrace("Create<{0}>({1},{2}) Started", typeof(T).Name, tenantKey, item.Id);
        
        ValidateIdentity(item);

        await Initialize();

        ApplyPartitionKey(tenantKey, item);
        
        item.CreatedAtUtc = DateTimeOffset.UtcNow;
        item.UpdatedAtUtc = DateTimeOffset.UtcNow;

        var options = new ItemRequestOptions
        {
            EnableContentResponseOnWrite = false
        };

        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        await container.CreateItemAsync(item, GetPartitionKey(tenantKey), options, cancellationToken);
        
        _logger.LogTrace("Create<{0}>({1},{2}) Finished", typeof(T).Name, tenantKey, item.Id);
    }

    public async Task UpdateAsync(string tenantKey, string id, T item, CancellationToken cancellationToken = default)
    {
        _logger.LogTrace("Update<{0}>({1},{2}) Started", typeof(T).Name, tenantKey, id);
        
        ValidateIdentity(item);

        await Initialize();

        ApplyPartitionKey(tenantKey, item);
        
        item.UpdatedAtUtc = DateTimeOffset.UtcNow;

        var options = new ItemRequestOptions
        {
            EnableContentResponseOnWrite = false
        };

        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        await container.ReplaceItemAsync(item, id, GetPartitionKey(tenantKey), options, cancellationToken);
        
        _logger.LogTrace("Update<{0}>({1},{2}) Finished", typeof(T).Name, tenantKey, id);
    }

    public async Task DeleteAsync(string tenantKey, string id, CancellationToken cancellationToken = default)
    {
        await Initialize();

        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        await container.DeleteItemAsync<T>(id, GetPartitionKey(tenantKey), cancellationToken: cancellationToken);
    }

    public async Task UpsertAsync(string tenantKey, T item, CancellationToken cancellationToken = default)
    {
        _logger.LogTrace("Upsert<{0}>({1},{2}) Started", typeof(T).Name, tenantKey, item.Id);
        
        ValidateIdentity(item);

        await Initialize();

        ApplyPartitionKey(tenantKey, item);

        item.UpdatedAtUtc = DateTimeOffset.UtcNow;

        var options = new ItemRequestOptions
        {
            EnableContentResponseOnWrite = false
        };

        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        await container.UpsertItemAsync(item, GetPartitionKey(tenantKey), options, cancellationToken);
        
        _logger.LogTrace("Upsert<{0}>({1},{2}) Finished", typeof(T).Name, tenantKey, item.Id);
    }

    public async Task UpsertAsync(string tenantKey, IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        await Initialize();

        var options = new ItemRequestOptions
        {
            EnableContentResponseOnWrite = false
        };

        var container = _client.GetDatabase(DatabaseId).GetContainer(CollectionId);
        var partitionKeyObject = GetPartitionKey(tenantKey);
        foreach (var item in items)
        {
            ValidateIdentity(item);
            ApplyPartitionKey(tenantKey, item);
            await container.UpsertItemAsync(item, partitionKeyObject, options, cancellationToken);
        }
    }

    private void ValidateGenericTypeIsSealed()
    {
        if (!typeof(T).IsSealed)
            throw new InvalidOperationException("Generic type must be sealed");
    }

    private void ValidateIdentity(T? item)
    {
        if (item == null)
            throw new ArgumentNullException(nameof(item));
        if (string.IsNullOrWhiteSpace(item.Id))
            throw new InvalidOperationException("Item must have an identity");
    }

    private void ApplyPartitionKey(string partitionKey, T item)
    {
        item.DomainName = DomainName;
        item.TenantKey = partitionKey;
        item.TypeName = typeof(T).Name;
    }

    private async Task CreateDatabaseIfNotExistsAsync()
    {
        await _client.CreateDatabaseIfNotExistsAsync(DatabaseId);
    }

    private async Task CreateCollectionIfNotExistsAsync()
    {
        var db = _client.GetDatabase(DatabaseId);
        var properties = new ContainerProperties(CollectionId, new[]{"/domain_name", "/type_name", "/tenant_key"});
        
        /*
        var indexingPolicy = properties.IndexingPolicy;
        indexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
        indexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/domain_name/?" });
        indexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/type_name/?" });
        indexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/tenant_key/?" });
        */

        await db.CreateContainerIfNotExistsAsync(properties, ThroughputProperties.CreateAutoscaleThroughput(_throughput));
    }
}

public class StorageServiceOptions
{
    public static readonly string OptionsSectionName = "StorageService";
    
    public string? Endpoint { get; set; }
    
    public string? Key { get; set; }
    
    public string? DatabaseName { get; set; }
    
    public string? CollectionName { get; set; }
    
    public string? DomainName { get; set; }
}