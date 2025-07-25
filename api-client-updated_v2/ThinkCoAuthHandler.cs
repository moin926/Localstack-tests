using System;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class ThinkCoAuthHandler : DelegatingHandler
{
    private readonly IConfiguration _configuration;

    // Single HttpClient instance for auth calls
    private readonly HttpClient    _authHttpClient;

    private string    _jwtToken;
    private DateTime  _jwtTokenExpiryUtc;
    private readonly SemaphoreSlim _refreshLock = new SemaphoreSlim(1,1);

    public ThinkCoAuthHandler(IConfiguration configuration)
    {
        _configuration    = configuration;

        // Instantiate your own HttpClient here
        _authHttpClient   = new HttpClient
        {
            BaseAddress = new Uri(_configuration["ThinkCo:Url"])
        };
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        bool useMockClients = _configuration.GetValue<bool>("UseMockClients");
        string path         = request.RequestUri.AbsolutePath;

        // 1) If mocking, skip auth logic entirely
        if (useMockClients)
            return await base.SendAsync(request, cancellationToken);

        // 2) If this *is* the /auth call, bypass token attachment & refresh
        if (path.EndsWith("/auth", StringComparison.OrdinalIgnoreCase))
            return await base.SendAsync(request, cancellationToken);

        // 3) Ensure we have a valid, unexpired JWT
        if (string.IsNullOrEmpty(_jwtToken) || DateTime.UtcNow >= _jwtTokenExpiryUtc)
        {
            await _refreshLock.WaitAsync(cancellationToken);
            try
            {
                if (string.IsNullOrEmpty(_jwtToken) || DateTime.UtcNow >= _jwtTokenExpiryUtc)
                {
                    // Instantiate a brand‑new ThinkCoClient pointing at the same URL
                    var thinkCoClient = new ThinkCoClient(_authHttpClient);

                    // Call the generated AuthenticateAsync(...) method
                    var authResponse = await thinkCoClient.AuthenticateAsync(
                        _configuration["ThinkCo:ClientId"],
                        _configuration["ThinkCo:ClientSecret"],
                        _configuration["ThinkCo:Username"],
                        _configuration["ThinkCo:Password"]
                    );

                    _jwtToken = authResponse.Token;

                    // Decode expiry from the JWT exp claim
                    var jwtHandler       = new JwtSecurityTokenHandler();
                    var jwtSecurityToken = jwtHandler.ReadJwtToken(_jwtToken);
                    _jwtTokenExpiryUtc   = jwtSecurityToken.ValidTo;
                }
            }
            finally
            {
                _refreshLock.Release();
            }
        }

        // 4) Attach Bearer and do the real request
        request.Headers.Authorization =
            new AuthenticationHeaderValue("Bearer", _jwtToken);

        var response = await base.SendAsync(request, cancellationToken);

        // 5) If we get a 401, clear token and retry once
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            _jwtToken = null;
            response.Dispose();

            var retryRequest = await CloneHttpRequestMessageAsync(request);
            return await base.SendAsync(retryRequest, cancellationToken);
        }

        return response;
    }

    private static async Task<HttpRequestMessage> CloneHttpRequestMessageAsync(
        HttpRequestMessage original)
    {
        var clone = new HttpRequestMessage(original.Method, original.RequestUri)
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
                clone.Content.Headers
                     .TryAddWithoutValidation(contentHeader.Key, contentHeader.Value);
        }

        return clone;
    }
}