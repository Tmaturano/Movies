
using Dapper;
using Movies.Application.Database;

namespace Movies.Application.Repositories;

public class RatingRepository : IRatingRepository
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    public RatingRepository(IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<float?> GetRatingAsync(Guid movieId, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        return await connection.QueryFirstOrDefaultAsync<float?>(new CommandDefinition("""
            select round(avg(rating), 1)
            from ratings
            where movieId = @MovieId;
            """, new { MovieId = movieId }, cancellationToken: cancellationToken));
    }

    public async Task<(float? Rating, int? UserRating)> GetRatingAsync(Guid movieId, Guid userId, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        return await connection.QueryFirstOrDefaultAsync<(float?, int?)>(new CommandDefinition("""
            select round(avg(rating), 1),
                (select rating
                from ratings
                where movieId = @MovieId and userId = @UserId
                limit 1)
            from ratings
            where movieId = @MovieId
            group by rating;
            """, new { MovieId = movieId, UserId = userId }, cancellationToken: cancellationToken));
    }

    public async Task<bool> RateMovieAsync(Guid movieId, int rating, Guid userId, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        var result = await connection.ExecuteAsync(new CommandDefinition("""
            insert into ratings (movieId, userId, rating)
            values (@MovieId, @UserId, @Rating)
            on conflict (movieId, userId) do update
            set rating = @Rating;
            """, new { MovieId = movieId, UserId = userId, Rating = rating }, cancellationToken: cancellationToken));

        return result > 0;
    }
}
