using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;

namespace Common.Data.TableStorage;

 public class TableStorageService
{
    private readonly TableServiceClient _tableServiceClient;

    public TableStorageService(TableServiceClient tableServiceClient)
    {
        _tableServiceClient = tableServiceClient;
    }

    public async Task Initialize<T>()
    {
        var cloudTable = await GetCloudTable(typeof(T).Name);
        await cloudTable.CreateIfNotExistsAsync();
    }
    
    public async Task<T?> Get<T>(string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        return await Get<T>(typeof(T).Name, partitionKey, rowKey);
    }

    public async Task<List<T>> Query<T>(int limit, Expression<Func<T, bool>>? where, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        return await QueryExpressionFilter(typeof(T).Name, limit, where, select);
    }

    public async Task<List<T>> Query<T>(Expression<Func<T, bool>>? where, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        return await QueryExpressionFilter(typeof(T).Name, null, where, select);
    }

    public async Task<List<T>> Query<T>(int limit, string? filter, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        return await QueryStringFilter<T>(typeof(T).Name, limit, filter, select);
    }

    public async Task<List<T>> Query<T>(string? filter = null, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        return await QueryStringFilter<T>(typeof(T).Name, null, filter, select);
    }

    public async Task Upsert<T>(T entity) where T : ITableEntity
    {
        await Upsert(typeof(T).Name, entity);
    }

    public async Task Delete<T>(T entity) where T : ITableEntity
    {
        await Delete(typeof(T).Name, entity);
    }

    public async Task<T?> Get<T>(string tableName, string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        return await QuerySingleExpressionFilter<T>(tableName, x => x.PartitionKey == partitionKey && x.RowKey == rowKey);
    }

    private async Task<List<T>> QueryExpressionFilter<T>(string tableName, int? limit = null, Expression<Func<T, bool>>? where = null, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        where ??= item => true;
        
        var maxPerPages = limit is < 1000 ? limit : 1000;

        var tableClient = await GetCloudTable(tableName);
        
        var query = tableClient.QueryAsync(where, maxPerPages, select);
        
        return await ExecuteTableQuery(query, limit);
    }

    private async Task<List<T>> QueryStringFilter<T>(string tableName, int? limit = null, string? filter = null, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        var maxPerPages = limit is < 1000 ? limit : 1000;

        var tableClient = await GetCloudTable(tableName);
        
        var query = tableClient.QueryAsync<T>(filter, maxPerPages, select);            
        
        return await ExecuteTableQuery(query, limit);
    }
    
    private async Task<T?> QuerySingleExpressionFilter<T>(string tableName, Expression<Func<T, bool>>? where = null, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        where ??= item => true;

        var tableClient = await GetCloudTable(tableName);
        
        var query = tableClient.QueryAsync(where, 1, select);
        
        return await ExecuteSingleQuery(query);
    }

    private async Task<T?> QuerySingleStringFilter<T>(string tableName, string? filter = null, IEnumerable<string>? select = null) where T : class, ITableEntity, new()
    {
        var tableClient = await GetCloudTable(tableName);
        
        var query = tableClient.QueryAsync<T>(filter, 1, select);            
        
        return await ExecuteSingleQuery(query);
    }        
    
    public async Task Upsert<T>(string tableName, T entity) where T : ITableEntity
    {
        var cloudTable = await GetCloudTable(tableName);
        await cloudTable.UpsertEntityAsync(entity, TableUpdateMode.Replace);
    }

    public async Task Delete<T>(string tableName, T entity) where T : ITableEntity
    {
        var cloudTable = await GetCloudTable(tableName);
        await cloudTable.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
    }

    private async Task<TableClient> GetCloudTable(string tableName)
    {
        var client = _tableServiceClient.GetTableClient(tableName);
        return await Task.FromResult(client);
    }
    
    private async Task<List<T>> ExecuteTableQuery<T>(AsyncPageable<T> query, int? limit = null) where T : ITableEntity, new()
    {
        var outputBuffer = new List<T>();

        await foreach (var page in query.AsPages())
        {
            outputBuffer.AddRange(page.Values);

            if (limit.HasValue && outputBuffer.Count >= limit.Value)
                break;
        }

        if (limit.HasValue)
            outputBuffer = outputBuffer.Take(limit.Value).ToList();

        return outputBuffer;
    }        
    
    private async Task<T?> ExecuteSingleQuery<T>(AsyncPageable<T> query) where T : ITableEntity, new()
    {
        await foreach (var page in query.AsPages())
        {
            foreach (var value in page.Values)
            {
                return value;
            }
        }
        
        return default;
    }        
}