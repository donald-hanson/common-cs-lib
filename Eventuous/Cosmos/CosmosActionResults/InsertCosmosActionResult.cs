using Eventuous.Subscriptions.Context;

namespace Common.Eventuous.Cosmos.CosmosActionResults;

internal class InsertCosmosActionResult<T> : CosmosActionResult<T> where T : IDocumentEntity, new()
{
    private readonly string _partitionKey;
    private readonly string _id;
    private readonly Action<T> _action;

    public InsertCosmosActionResult(string partitionKey, string id, Action<T> action)
    {
        _partitionKey = partitionKey;
        _id = id;
        _action = action;   
    }

    public override Task Execute(IBaseConsumeContext context, IStorageService<T> storageService, CancellationToken cancellationToken)
    {
        var document = new T
        {
            Id = _id,
            CreatedAtUtc = DateTimeOffset.UtcNow
        };
        _action(document);
        document.UpdatedAtUtc = DateTimeOffset.UtcNow;
        document.GlobalPosition = context.GlobalPosition;
        
        return storageService.UpsertAsync(_partitionKey, document, cancellationToken);
    }
}