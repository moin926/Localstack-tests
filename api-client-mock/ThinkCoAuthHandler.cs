using System;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class ThinkCoAuthHandler : DelegatingHandler
{
    private readonly IConfiguration      _configuration;
    private readonly IHttpClientFactory  _httpClientFactory;
    private string                       _jwtToken;
    private DateTime                     _jwtTokenExpiryUtc;
    private readonly SemaphoreSlim       _refreshLock = new SemaphoreSlim(1,1);

    public ThinkCoAuthHandler(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory)
    {
        _configuration     = configuration;
        _httpClientFactory = httpClientFactory;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        bool useMockClients = _configuration.GetValue<bool>("UseMockClients");
        string path         = request.RequestUri.AbsolutePath;

        // 1) Bypass auth logic entirely if mocking
        if (useMockClients)
            return await base.SendAsync(request, cancellationToken);

        // 2) If this is the /auth call itself, skip token attachment & refresh logic
        if (path.EndsWith("/auth", StringComparison.OrdinalIgnoreCase))
            return await base.SendAsync(request, cancellationToken);

        // 3) Ensure we have a valid, unexpired token
        if (string.IsNullOrEmpty(_jwtToken) || DateTime.UtcNow >= _jwtTokenExpiryUtc)
        {
            await _refreshLock.WaitAsync(cancellationToken);
            try
            {
                if (string.IsNullOrEmpty(_jwtToken) || DateTime.UtcNow >= _jwtTokenExpiryUtc)
                {
                    var client = new ThinkCoClient(
                        _httpClientFactory.CreateClient(
                            typeof(IThinkCoClient).FullName));

                    var authResponse = await client.AuthenticateAsync(
                        _configuration["ThinkCo:ClientId"],
                        _configuration["ThinkCo:ClientSecret"],
                        _configuration["ThinkCo:Username"],
                        _configuration["ThinkCo:Password"]
                    );

                    _jwtToken = authResponse.Token;

                    var handler = new JwtSecurityTokenHandler();
                    var jwt     = handler.ReadJwtToken(_jwtToken);
                    _jwtTokenExpiryUtc = jwt.ValidTo;
                }
            }
            finally
            {
                _refreshLock.Release();
            }
        }

        // 4) Attach the bearer token
        request.Headers.Authorization =
            new AuthenticationHeaderValue("Bearer", _jwtToken);

        // 5) First attempt
        var response = await base.SendAsync(request, cancellationToken);

        // 6) If 401, clear token & retry once
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            _jwtToken = null;
            response.Dispose();

            var retryRequest = await CloneHttpRequestMessageAsync(request);
            var freshToken = (await SendAsync(
                 new HttpRequestMessage(HttpMethod.Get, "/auth"),
                 cancellationToken))
                 .RequestMessage.Headers.Authorization.Parameter;
            retryRequest.Headers.Authorization =
                new AuthenticationHeaderValue("Bearer", freshToken);

            return await base.SendAsync(retryRequest, cancellationToken);
        }

        return response;
    }

    private static async Task<HttpRequestMessage> CloneHttpRequestMessageAsync(
        HttpRequestMessage original)
    {
        var clone = new HttpRequestMessage(
            original.Method, original.RequestUri)
        {
            Version = original.Version
        };
        foreach (var header in original.Headers)
            clone.Headers.TryAddWithoutValidation(header.Key, header.Value);

        if (original.Content != null)
        {
            using var ms = new MemoryStream();
            await original.Content.CopyToAsync(ms);
            ms.Position = 0;
            clone.Content = new StreamContent(ms);
            foreach (var contentHeader in original.Content.Headers)
                clone.Content.Headers.TryAddWithoutValidation(
                    contentHeader.Key, contentHeader.Value);
        }

        return clone;
    }
}