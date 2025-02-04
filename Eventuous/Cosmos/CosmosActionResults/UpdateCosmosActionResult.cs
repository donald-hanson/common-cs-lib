using Eventuous.Subscriptions.Context;

namespace Common.Eventuous.Cosmos.CosmosActionResults;

internal class UpdateCosmosActionResult<T> : CosmosActionResult<T> where T : IDocumentEntity
{
    private readonly string _partitionKey;
    private readonly string _id;
    private readonly Func<T, CancellationToken, Task> _action;

    private UpdateCosmosActionResult(string partitionKey, string id)
    {
        _partitionKey = partitionKey;
        _id = id;        
    } 

    public UpdateCosmosActionResult(string partitionKey, string id, Action<T> action) : this(partitionKey, id)
    {
        _action = (t, _) =>
        {
            action(t);
            return Task.CompletedTask;
        };   
    }
    
    public UpdateCosmosActionResult(string partitionKey, string id, Func<T, CancellationToken, Task> action) : this(partitionKey, id)
    {
        _action = action;
    }
    
    public UpdateCosmosActionResult(string partitionKey, string id, Func<T, Task> action) : this(partitionKey, id)
    {
        _action = (t, _) => action(t);
    }    

    public override async Task Execute(IBaseConsumeContext context, IStorageService<T> storageService, CancellationToken cancellationToken)
    {
        var document = await storageService.GetAsync(_partitionKey, _id);
        if (document != null)
        {
            await _action(document, cancellationToken);
            document.GlobalPosition = context.GlobalPosition;
            
            await storageService.UpdateAsync(_partitionKey, _id, document, cancellationToken);
        }
    }
}