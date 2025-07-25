using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class ThinkCoMockHandler : DelegatingHandler
{
    private readonly IConfiguration _configuration;

    public ThinkCoMockHandler(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        if (_configuration.GetValue<bool>("UseMockClients"))
        {
            var path = request.RequestUri.AbsolutePath;

            if (path.EndsWith("/auth", StringComparison.OrdinalIgnoreCase))
            {
                var fake = new AuthenticateResponse {
                    Token     = "MOCK-JWT",
                    ExpiresIn = 3600
                };
                return Task.FromResult(CreateJson(fake));
            }

            if (request.Method == HttpMethod.Post
                && path.EndsWith("/NewBusiness", StringComparison.OrdinalIgnoreCase))
            {
                var fake = new NewBusinessResponse {
                    BusinessId = Guid.NewGuid().ToString(),
                    Status     = "MockCreated"
                };
                return Task.FromResult(CreateJson(fake));
            }

            if (request.Method == HttpMethod.Put
                && path.EndsWith("/Renewals", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }

            if (request.Method == HttpMethod.Delete
                && path.EndsWith("/Cancellation", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotImplemented)
            {
                Content = new StringContent($"No mock for {request.Method} {path}")
            });
        }

        return base.SendAsync(request, cancellationToken);
    }

    private static HttpResponseMessage CreateJson<T>(T dto)
    {
        var json = JsonSerializer.Serialize(dto);
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
        };
    }
}