using Dapper;

namespace Movies.Application.Database;

public class DbInitializer
{
    private readonly IDbConnectionFactory _dbConnectionFactory;
    public DbInitializer(IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }
    public async Task InitializeAsync()
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();

        await connection.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS movies (
                id UUID PRIMARY KEY,
                title TEXT NOT NULL,
                slug TEXT NOT NULL,
                yearofrelease integer not null);
            """);

        await connection.ExecuteAsync("""
            CREATE UNIQUE INDEX concurrently IF NOT EXISTS idx_movies_slug 
            ON movies
            USING BTREE(slug);
            """);

        await connection.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS genres (
                movieId UUID REFERENCES movies(id),
                name TEXT NOT NULL);
            """);
    }
}
