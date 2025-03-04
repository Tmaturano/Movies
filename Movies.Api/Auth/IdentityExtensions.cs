namespace Movies.Api.Auth;

public static class IdentityExtensions
{
    public const string UserIdClaimName = "userid";
    public static Guid? GetUserId(this HttpContext context)
    {
        var userId = context.User.Claims.FirstOrDefault(c => c.Type == UserIdClaimName)?.Value;        
        return userId is not null ? Guid.Parse(userId) : null;
    }
}
