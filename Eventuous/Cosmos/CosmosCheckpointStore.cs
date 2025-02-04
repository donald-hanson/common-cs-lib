using Eventuous.Subscriptions.Checkpoints;

namespace Common.Eventuous.Cosmos;

public class CosmosCheckpointStore : ICheckpointStore
{
    private readonly IStorageService<CosmosCheckpoint> _storageService;

    public CosmosCheckpointStore(IStorageService<CosmosCheckpoint> storageService)
    {
        _storageService = storageService;
    }
    
    public async ValueTask<Checkpoint> GetLastCheckpoint(string checkpointId, CancellationToken cancellationToken)
    {
        var cosmosCheckpoint = await _storageService.GetAsync("Default", checkpointId);
        if (cosmosCheckpoint?.GlobalPosition == null)
        {
            return Checkpoint.Empty(checkpointId);
        }
        return new Checkpoint(cosmosCheckpoint.Id, cosmosCheckpoint.GlobalPosition);
    }

    public async ValueTask<Checkpoint> StoreCheckpoint(Checkpoint checkpoint, bool force, CancellationToken cancellationToken)
    {
        var cosmosCheckpoint = new CosmosCheckpoint
        {
            Id = checkpoint.Id,
            TenantKey = "Default",
            GlobalPosition = checkpoint.Position
        };
        await _storageService.UpsertAsync("Default", cosmosCheckpoint);
        return checkpoint;
    }
}

public sealed record CosmosCheckpoint : Document
{
    
}