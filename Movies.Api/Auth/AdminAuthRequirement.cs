using Microsoft.AspNetCore.Authorization;
using System.Security.Claims;

namespace Movies.Api.Auth;

public class AdminAuthRequirement : IAuthorizationHandler, IAuthorizationRequirement
{
    private readonly string _apiKey;

    public AdminAuthRequirement(string apiKey)
    {
        _apiKey = apiKey;
    }

    /// <summary>
    /// Logic that handlesboth JWT claim or API Key value
    /// </summary>
    /// <param name="context"></param>
    /// <returns></returns>
    public Task HandleAsync(AuthorizationHandlerContext context)
    {
        if (context.User.HasClaim(AuthConstants.AdminUserClaimName, "true"))
        {
            context.Succeed(this);
            return Task.CompletedTask;
        }

        if (context.Resource is not HttpContext httpContext)
        {
            context.Fail();
            return Task.CompletedTask;
        }

        if (!httpContext.Request.Headers.TryGetValue(AuthConstants.ApiKeyHeaderName, out var extractedApiKey))
        {
            context.Fail();
            return Task.CompletedTask;
        }

        if (_apiKey != extractedApiKey)
        {
            context.Fail();
            return Task.CompletedTask;
        }
        
        var identity = (ClaimsIdentity)httpContext.User.Identity;
        //API Key tied to user ID
        //Hardcoded for example, but should be the user Id
        identity?.AddClaim(new Claim(IdentityExtensions.UserIdClaimName, Guid.Parse("68f1885c-d535-4e3f-8e5f-1032dae89c5e").ToString()));
        context.Succeed(this);
        return Task.CompletedTask;
    }
}
