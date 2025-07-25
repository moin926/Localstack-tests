using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;
var configuration = builder.Configuration;

// 1) Handlers & pipeline filter
services.AddTransient<ThinkCoAuthHandler>();
services.AddTransient<ThinkCoMockHandler>();
services.AddTransient<TrackM8AuthHandler>();
services.AddTransient<TrackM8MockHandler>();
services.AddSingleton<IHttpMessageHandlerBuilderFilter, ClientPipelineFilter>();

// 2) NSwag-generated clients with handlers
services
  .AddHttpClient<IThinkCoClient, ThinkCoClient>(c =>
      c.BaseAddress = new Uri(configuration["ThinkCo:Url"]))
  .AddHttpMessageHandler<ThinkCoAuthHandler>();

services
  .AddHttpClient<ITrackM8Client, TrackM8Client>(c =>
      c.BaseAddress = new Uri(configuration["TrackM8:Url"]))
  .AddHttpMessageHandler<TrackM8AuthHandler>();

var app = builder.Build();
app.Run();