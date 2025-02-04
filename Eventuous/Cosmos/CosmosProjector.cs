using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Eventuous;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Common.Eventuous.Cosmos.CosmosActionResults;
using static Eventuous.Subscriptions.Diagnostics.SubscriptionsEventSource;

namespace Common.Eventuous.Cosmos;

public abstract record Document : IDocumentEntity
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("domain_name")]
    public string DomainName { get; set; } = null!;

    [JsonPropertyName("type_name")]
    public string TypeName { get; set; } = null!;

    [JsonPropertyName("tenant_key")]
    public string? TenantKey { get; set; }

    [JsonPropertyName("global_position")]
    public ulong? GlobalPosition { get; set; }
    
    [JsonPropertyName("created_at_utc")]
    public DateTimeOffset CreatedAtUtc { get; set; }
    
    [JsonPropertyName("updated_at_utc")]
    public DateTimeOffset UpdatedAtUtc { get; set; }    
}

public abstract class CosmosActionResult<T> where T : IDocumentEntity
{
    public virtual Task Execute(IBaseConsumeContext context, IStorageService<T> storageService, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

public abstract class CosmosProjector<T> : BaseEventHandler where T : IDocumentEntity, new()
{
    private readonly IStorageService<T> _storageService;
    
    readonly Dictionary<Type, ProjectUntypedEvent> _handlers = new();
    readonly TypeMapper                            _map;

    protected CosmosProjector(IStorageService<T> storageService, TypeMapper? typeMap = null) {
        _storageService = storageService;
        _map       = typeMap ?? TypeMap.Instance;
    }

    /// <summary>
    /// Register a handler for a particular event type
    /// </summary>
    /// <param name="handler">Function which handles an event</param>
    /// <typeparam name="TEvent">Event type</typeparam>
    protected void On<TEvent>(ProjectTypedEvent<T, TEvent> handler) where TEvent : class {
        if (!_handlers.TryAdd(typeof(TEvent), x => HandleInternal(x, handler))) {
            throw new ArgumentException($"Type {typeof(TEvent).Name} already has a handler");
        }

        if (!_map.TryGetTypeName<TEvent>(out _)) {
            Log.MessageTypeNotRegistered<TEvent>();
        }
    }

    protected void On<TEvent>(Func<MessageConsumeContext<TEvent>, IStorageService<T>, CancellationToken, Task> handler)
        where TEvent : class
    {
        On<TEvent>(ctx =>
        {
            var op = new CosmosProjectOperation<T>((s, c) => handler(ctx, s, c));
            return ValueTask.FromResult(op);
        });
    }
    
    protected void On<TEvent>(Func<TEvent, CancellationToken, Task<CosmosActionResult<T>>> handler)
        where TEvent : class
    {
        On<TEvent>(ctx =>
        {
            var op = new CosmosProjectOperation<T>(async (s, c) =>
            {
                var actionResult = await handler(ctx.Message, c);
                await actionResult.Execute(ctx, s, c);
            });
            return ValueTask.FromResult(op);
        });
    }
    
    protected void On<TEvent>(Func<TEvent, IMessageConsumeContext<TEvent>, CosmosActionResult<T>> handler)
        where TEvent : class
    {
        On<TEvent>(ctx =>
        {
            var op = new CosmosProjectOperation<T>(async (s, c) =>
            {
                var actionResult = handler(ctx.Message, ctx);
                await actionResult.Execute(ctx, s, c);
            });
            return ValueTask.FromResult(op);
        });
    }
    
    protected void On<TEvent>(Func<TEvent, CosmosActionResult<T>> handler)
        where TEvent : class
    {
        On<TEvent>(ctx =>
        {
            var op = new CosmosProjectOperation<T>(async (s, c) =>
            {
                var actionResult = handler(ctx.Message);
                if (c.IsCancellationRequested)
                    return;
                await actionResult.Execute(ctx, s, c);
            });
            return ValueTask.FromResult(op);
        });
    }    
    
    protected static CosmosActionResult<T> Insert(string partitionKey, string id, Action<T> action)
    {
        return new InsertCosmosActionResult<T>(partitionKey, id, action);
    }
    
    protected static CosmosActionResult<T> Update(string partitionKey, string id, Action<T> action)
    {
        return new UpdateCosmosActionResult<T>(partitionKey, id, action);
    }
    
    protected static CosmosActionResult<T> Update(string partitionKey, string id, Func<T, Task> action)
    {
        return new UpdateCosmosActionResult<T>(partitionKey, id, action);
    }    
    
    protected static CosmosActionResult<T> Update(string partitionKey, string id, Func<T, CancellationToken, Task> action)
    {
        return new UpdateCosmosActionResult<T>(partitionKey, id, action);
    }    
    
    protected static CosmosActionResult<T> Upsert(string partitionKey, string id, Action<T> action)
    {
        return new UpsertCosmosActionResult<T>(partitionKey, id, action);
    }    
    
    protected static CosmosActionResult<T> Noop()
    {
        return new NoopCosmosActionResult<T>();
    }    

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<CosmosProjectOperation<T>> HandleInternal<TEvent>(
        IMessageConsumeContext       context,
        ProjectTypedEvent<T, TEvent> handler
    )
        where TEvent : class {
        return context.Message is not TEvent
            ? NoHandler()
            : HandleTypedEvent();

        ValueTask<CosmosProjectOperation<T>> HandleTypedEvent() {
            var typedContext = context as MessageConsumeContext<TEvent> ?? new MessageConsumeContext<TEvent>(context);
            return handler(typedContext);
        }

        ValueTask<CosmosProjectOperation<T>> NoHandler()
        {
            return ValueTask.FromResult(new CosmosProjectOperation<T>((_, _) => Task.CompletedTask));
        }
    }

    public override async ValueTask<EventHandlingStatus> HandleEvent(IMessageConsumeContext context)
    {
        if (!_handlers.TryGetValue(context.Message!.GetType(), out var handler))
        {
            return EventHandlingStatus.Ignored;
        }

        var update = await handler(context).ConfigureAwait(false);

        await update.Execute(_storageService, context.CancellationToken);
        
        return EventHandlingStatus.Success;
    }

    private delegate ValueTask<CosmosProjectOperation<T>> ProjectUntypedEvent(IMessageConsumeContext evt);
}

public delegate ValueTask<CosmosProjectOperation<T>> ProjectTypedEvent<T, TEvent>(MessageConsumeContext<TEvent> consumeContext)
    where T : IDocumentEntity where TEvent : class;

public record CosmosProjectOperation<T>(Func<IStorageService<T>, CancellationToken, Task> Execute);
