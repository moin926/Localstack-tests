using System;
using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public class ClientPipelineFilter : IHttpMessageHandlerBuilderFilter
{
    private readonly IServiceProvider _services;
    private readonly IConfiguration   _configuration;

    public ClientPipelineFilter(
        IServiceProvider services,
        IConfiguration configuration)
    {
        _services       = services;
        _configuration  = configuration;
    }

    public Action<HttpMessageHandlerBuilder> Configure(
        Action<HttpMessageHandlerBuilder> next) =>
      builder =>
      {
        next(builder);

        bool useMock = _configuration.GetValue<bool>("UseMockClients");
        string name  = builder.Name;

        if (name.Contains(nameof(IThinkCoClient)))
        {
            if (useMock)
                builder.AdditionalHandlers.Insert(
                    0, _services.GetRequiredService<ThinkCoMockHandler>());
            else
                builder.AdditionalHandlers.Insert(
                    0, _services.GetRequiredService<ThinkCoAuthHandler>());
        }
        else if (name.Contains(nameof(ITrackM8Client)))
        {
            if (useMock)
                builder.AdditionalHandlers.Insert(
                    0, _services.GetRequiredService<TrackM8MockHandler>());
            else
                builder.AdditionalHandlers.Insert(
                    0, _services.GetRequiredService<TrackM8AuthHandler>());
        }
      };
}