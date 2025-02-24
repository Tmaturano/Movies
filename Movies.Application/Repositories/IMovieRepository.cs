using Movies.Application.Models;

namespace Movies.Application.Repositories;

public interface IMovieRepository
{
    Task<bool> CreateAsync(Movie movie, CancellationToken cancellationToken);
    Task<Movie?> GetByIdAsync(Guid id, CancellationToken cancellationToken);
    Task<Movie?> GetBySlugAsync(string slug, CancellationToken cancellationToken);
    Task<IEnumerable<Movie>> GetAllAsync(CancellationToken cancellationToken);
    Task<bool> UpdateAsync(Movie movie, CancellationToken cancellationToken);
    Task<bool> DeleteByIdAsync(Guid id, CancellationToken cancellationToken);
    Task<bool> ExistsByIdAsync(Guid id, CancellationToken cancellationToken);
}
