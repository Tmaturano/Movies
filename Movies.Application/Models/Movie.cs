using System.Text.RegularExpressions;

namespace Movies.Application.Models;

public partial class Movie
{
    public required Guid Id { get; init; }
    public required string Title { get; init; }
    public string Slug => GenerateSlug();
    public float? Rating { get; set; }
    public int? UserRating { get; set; }
    public required int YearOfRelease { get; init; }
    public required List<string> Genres { get; init; } = [];

    private string GenerateSlug()
    {
        var sluggedTitle = SlugRegex().Replace(Title, string.Empty)
            .ToLowerInvariant().Replace(" ", "-");

        return $"{sluggedTitle}-{YearOfRelease}";
    }

    [GeneratedRegex("[^0-9A-Za-z _-]", RegexOptions.NonBacktracking, 10)]
    private static partial Regex SlugRegex();
}
