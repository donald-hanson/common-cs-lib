using System.Linq.Expressions;
using System.Reflection;
using EventStore.Client;
using Eventuous;
using Eventuous.EventStore;
using Eventuous.EventStore.Producers;
using Eventuous.EventStore.Subscriptions;
using Eventuous.Subscriptions;
using Common.Eventuous.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StreamSubscription = Eventuous.EventStore.Subscriptions.StreamSubscription;

namespace Common.Eventuous;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSharedInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddEventStoreClient(configuration["EventStore:ConnectionString"]!);
        services.AddEventStore<EsdbEventStore>();

        services.AddSingleton<IDocumentClientProvider, DocumentClientProvider>();
        services.Configure<StorageServiceOptions>(configuration.GetSection(StorageServiceOptions.OptionsSectionName));
        services.AddSingleton(typeof(IStorageService<>), typeof(StorageService<>));
        services.AddCheckpointStore<CosmosCheckpointStore>();
        
        services.AddProducer<EventStoreProducer>();

        return services;
    }
    
    public static IServiceCollection AddStreamNameMapping<TId>(this IServiceCollection services, Expression<Func<TId, StreamName>> map) where TId : AggregateId
    {
        services.AddSingleton<IStreamNameMappingRegistration>(new StreamNameMappingRegistration
        {
            Type = typeof(TId),
            Map = map
        });

        services.TryAddSingleton(sp =>
        {
            var streamNameMap = new StreamNameMap();

            var registrations = sp.GetServices<IStreamNameMappingRegistration>();

            var openGenericRegisterMethod = streamNameMap.GetType().GetMethod("Register", BindingFlags.Instance | BindingFlags.Public);
            
            foreach (var registration in registrations)
            {
                var closedGenericRegisterMethod = openGenericRegisterMethod!.MakeGenericMethod(registration.Type);

                closedGenericRegisterMethod.Invoke(streamNameMap, new object?[] {registration.Map.Compile()});
            }

            return streamNameMap;
        });

        return services;
    }

    public static IServiceCollection AddIntegrationSubscription<THandler>(this IServiceCollection services, string sourceDomain, string targetDomain, StreamName streamName) where THandler : class, IEventHandler
    {
        services.AddSubscription<StreamSubscription, StreamSubscriptionOptions>(
            $"Subscription.{sourceDomain}.Integration.{targetDomain}",
            builder =>
            {
                builder
                    .Configure(x =>
                    {
                        x.StreamName = streamName;
                    })
                    .UseCheckpointStore<CosmosCheckpointStore>()
                    .AddEventHandler<THandler>();
            }); 
        return services;
    }
    
    private class StreamNameMappingRegistration : IStreamNameMappingRegistration
    {
        public Type Type { get; init; } = null!;
        public LambdaExpression Map { get; init; } = null!;
    }
    
    internal interface IStreamNameMappingRegistration
    {
        public Type Type { get; }
        public LambdaExpression Map { get; }        
    }    
}