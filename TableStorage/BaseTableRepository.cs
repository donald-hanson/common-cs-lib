using Azure.Data.Tables;

namespace Common.Data.TableStorage;

public abstract class BaseTableRepository<T> where T : class, ITableEntity, new()
{
    private readonly TableStorageService _tableStorageService;

    private readonly Lazy<Task> _init;
    private readonly string[] IdentityFields = {nameof(ITableEntity.PartitionKey), nameof(ITableEntity.RowKey)};

    public BaseTableRepository(TableStorageService tableStorageService)
    {
        _tableStorageService = tableStorageService;
        _init = new Lazy<Task>(InternalInitialize);
    }

    protected async Task Initialize()
    {
        await _init.Value;
    }

    private async Task InternalInitialize()
    {
        await _tableStorageService.Initialize<T>();
    }

    public async Task<List<T>> GetByPartitionKeyAsync(string partitionKey)
    {
        await Initialize();
        return await _tableStorageService.Query<T>(x => x.PartitionKey == partitionKey);
    }

    public async Task<List<T>> GetIdentitiesAsync()
    {
        await Initialize();
        return await _tableStorageService.Query<T>((string?)null, IdentityFields);
    }

    public async Task<List<T>> GetIdentitiesByPartitionKeyAsync(string partitionKey)
    {
        await Initialize();
        return await _tableStorageService.Query<T>(x => x.PartitionKey == partitionKey, IdentityFields);
    }

    public async Task<List<T>> GetByPartitionKeyAndRowKeysAsync(string partitionKey, HashSet<string> rowKeys)
    {
        await Initialize();

        var rows = await _tableStorageService.Query<T>(x => x.PartitionKey == partitionKey);

        return rows.Where(r => rowKeys.Contains(r.RowKey)).ToList();
    }
    
    public async Task<T?> GetByPartitionKeyAndRowKeyAsync(string partitionKey, string rowKey)
    {
        await Initialize();

        return await  _tableStorageService.Get<T>(partitionKey, rowKey);
    }    

    public async Task UpsertAsync(T entity)
    {
        await Initialize();
        await _tableStorageService.Upsert(entity);
    }

    public async Task DeleteAsync(T entity)
    {
        await Initialize();
        await _tableStorageService.Delete(entity);
    }
    
    public async Task DeleteByPartitionKeyAsync(string partitionKey)
    {
        await Initialize();
        var entitiesToDelete = await GetIdentitiesByPartitionKeyAsync(partitionKey);
        foreach (var entity in entitiesToDelete)
        {
            await _tableStorageService.Delete(entity);
        }
    }    
}