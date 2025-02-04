using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;

namespace Common.Data.TableStorage;

public static class Startup
{
    public static IServiceCollection AddTableStorage(this IServiceCollection services)
    {
        services.AddSingleton(provider =>
        {
            var tableServiceClient = new TableServiceClient("UseDevelopmentStorage=true");
            return new TableStorageService(tableServiceClient);
        });
        return services;
    }
}