using Eventuous.Subscriptions.Context;

namespace Common.Eventuous.Cosmos.CosmosActionResults;

internal class UpsertCosmosActionResult<T> : CosmosActionResult<T> where T : IDocumentEntity, new()
{
    private readonly string _partitionKey;
    private readonly string _id;
    private readonly Action<T> _action;

    public UpsertCosmosActionResult(string partitionKey, string id, Action<T> action)
    {
        _partitionKey = partitionKey;
        _id = id;
        _action = action;   
    }

    public override async Task Execute(IBaseConsumeContext context, IStorageService<T> storageService, CancellationToken cancellationToken)
    {
        var document = await storageService.GetAsync(_partitionKey, _id, cancellationToken);
        if (document == null)
        {
            document = new T
            {
                Id = _id,
                CreatedAtUtc = DateTime.UtcNow
            };
        }

        _action(document);
        document.UpdatedAtUtc = DateTime.UtcNow;
        document.GlobalPosition = context.GlobalPosition;
        await storageService.UpsertAsync(_partitionKey, document, cancellationToken);
    }
}