using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class TrackM8AuthHandler : DelegatingHandler
{
    private readonly IConfiguration _configuration;

    public TrackM8AuthHandler(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        if (!_configuration.GetValue<bool>("UseMockClients"))
        {
            var user = _configuration["TrackM8:Username"];
            var pass = _configuration["TrackM8:Password"];
            var creds = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{user}:{pass}"));
            request.Headers.Authorization =
                new AuthenticationHeaderValue("Basic", creds);
        }

        return base.SendAsync(request, cancellationToken);
    }
}