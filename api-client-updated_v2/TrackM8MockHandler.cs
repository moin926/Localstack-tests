using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class TrackM8MockHandler : DelegatingHandler
{
    private readonly IConfiguration _configuration;

    public TrackM8MockHandler(IConfiguration configuration)
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

            if (request.Method == HttpMethod.Post
                && path.EndsWith("/Nb", StringComparison.OrdinalIgnoreCase))
            {
                var fake = new NbPolicyResponse {
                    PolicyNumber = "MOCK-123",
                    Status       = "MockIssued"
                };
                return Task.FromResult(CreateJson(fake));
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