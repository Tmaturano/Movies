using Dapper;
using Movies.Application.Database;
using Movies.Application.Models;

namespace Movies.Application.Repositories;

public class MovieRepository : IMovieRepository
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    public MovieRepository(IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<bool> CreateAsync(Movie movie, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        var result = await connection.ExecuteAsync(new CommandDefinition("""
            insert into movies (id, slug, title, yearofrelease)
            values(@Id, @Slug, @Title, @YearOfRelease);
            """, movie, transaction, cancellationToken: cancellationToken));

        if (result > 0)
        {
            foreach (var genre in movie.Genres)
            {
                await connection.ExecuteAsync(new CommandDefinition("""
                    insert into genres (movieId, name)
                    values(@MovieId, @Name)
                    """, new { MovieId = movie.Id, Name = genre }, transaction, cancellationToken: cancellationToken));
            }
        }

        transaction.Commit();

        return result > 0;
    }

    public async Task<bool> DeleteByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync(new CommandDefinition("""
            delete from genres
            where movieId = @MovieId;
            """, new { MovieId = id }, cancellationToken: cancellationToken));

        var result = await connection.ExecuteAsync(new CommandDefinition("""
            delete from movies
            where id = @Id;
            """, new { Id = id }, cancellationToken: cancellationToken));
        
        transaction.Commit();

        return result > 0;
    }

    public async Task<bool> ExistsByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);

        return await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
            select count(1) from movies where id = @Id;
            """, new { Id = id }, cancellationToken: cancellationToken));
    }

    public async Task<IEnumerable<Movie>> GetAllAsync(Guid? userId = default, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);

        var result = await connection.QueryAsync(new CommandDefinition("""
            select m.*, string_agg(distinct g.name, ',') as genres, round(avg(r.rating), 1) as rating, myr.rating as userrating
            from movies m 
            left join genres g on m.id = g.movieid
            left join ratings r on m.id = r.movieid
            left join ratings myr on m.id = myr.movieid and myr.userid = @userId
            group by id, userrating
            """, new { userId }, cancellationToken: cancellationToken));

        return result.Select(row => new Movie
        {
            Id = row.id,
            Title = row.title,
            YearOfRelease = row.yearofrelease,
            Rating = (float?)row.rating,
            UserRating = (int?)row.userrating,
            Genres = Enumerable.ToList(row.genres.Split(','))            
        });
    }

    public async Task<Movie?> GetByIdAsync(Guid id, Guid? userId = default, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);

        var movie = await connection.QueryFirstOrDefaultAsync<Movie>(new CommandDefinition("""
            select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
            from movies m
            left join ratings r on m.id = r.movieid
            left join ratings myr on m.id = myr.movieid and myr.userid = @userId
            where m.id = @Id
            group by m.id, userrating;
            """, new { Id = id, userId }, cancellationToken: cancellationToken));

        if (movie is null)
        {
            return null;
        }

        var genres = await connection.QueryAsync<string>(new CommandDefinition("""
            select name
            from genres
            where movieId = @MovieId;
            """, new { MovieId = id }, cancellationToken: cancellationToken));

        movie.Genres.AddRange(genres);

        return movie;
    }

    public async Task<Movie?> GetBySlugAsync(string slug, Guid? userId = default, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);

        var movie = await connection.QueryFirstOrDefaultAsync<Movie>(new CommandDefinition("""
            select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
            from movies m
            left join ratings r on m.id = r.movieid
            left join ratings myr on m.id = myr.movieid and myr.userid = @userId
            where m.slug = @Slug
            group by m.id, userrating;
            """, new { Slug = slug, userId }, cancellationToken: cancellationToken));

        if (movie is null)
        {
            return null;
        }

        var genres = await connection.QueryAsync<string>(new CommandDefinition("""
            select name
            from genres
            where movieId = @MovieId;
            """, new { MovieId = movie.Id }, cancellationToken: cancellationToken));

        movie.Genres.AddRange(genres);

        return movie;
    }

    public async Task<bool> UpdateAsync(Movie movie, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync(new CommandDefinition("""
            delete from genres where movieId = @MovieId;
            """, new { MovieId = movie.Id }, transaction, cancellationToken: cancellationToken));

        foreach (var genre in movie.Genres)
        {
            await connection.ExecuteAsync(new CommandDefinition("""
                insert into genres (movieId, name)
                values (@MovieId, @Name)
                """, new { MovieId = movie.Id, Name = genre }, transaction, cancellationToken: cancellationToken));
        }

        var result = await connection.ExecuteAsync(new CommandDefinition("""
            update movies
            set title = @Title, slug = @Slug, yearofrelease = @YearOfRelease
            where id = @Id;
            """, movie, transaction, cancellationToken: cancellationToken));

        transaction.Commit();
        return result > 0;
    }
}
