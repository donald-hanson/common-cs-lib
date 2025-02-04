namespace Common.Eventuous.Cosmos.CosmosActionResults;

internal class NoopCosmosActionResult<T> : CosmosActionResult<T> where T : IDocumentEntity, new()
{
    
}