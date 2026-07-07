using System.Text.Json;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using EsportTournamentsApp.Models;

namespace EsportTournamentsApp.Services;

public class TournamentService
{
    private readonly HttpClient _httpClient;
    private const string BaseUrl = "https://tourneyradar-api.vercel.app/v1/tournaments";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public TournamentService(HttpClient httpClient) => _httpClient = httpClient;

    public IObservable<List<RawTournament>> WatchTournaments(string category, string? country, string? format, TimeSpan interval)
    {
        return Observable.Interval(interval, TaskPoolScheduler.Default)
            .StartWith(0L)
            .SelectMany(_ =>
                Observable.FromAsync(() => Fetch(category, country, format))
                    .Catch<List<RawTournament>, Exception>(ex =>
                    {
                        Logger.Log("API ERROR", $"Fetch za '{category}' neuspesan: {ex.Message}");
                        return Observable.Return(new List<RawTournament>());
                    }));
    }

    private async Task<List<RawTournament>> Fetch(string category, string? country, string? format)
    {
        var url = $"{BaseUrl}?category={category}&limit=100";
        if (!string.IsNullOrEmpty(country))
            url += $"&country={country}";
        if (!string.IsNullOrEmpty(format))
            url += $"&format={format}";

        Logger.Log("API CALL", $"Povlaci turnire: category={category}, country={country ?? "any"}, format={format ?? "any"}");
        Logger.Log("API CALL", $"URL: {url}");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Logger.Log("API CALL", $"Response status: {response.StatusCode}");
        response.EnsureSuccessStatusCode();
        var responseString = await response.Content.ReadAsStringAsync(cts.Token);
        Logger.Log("API CALL", $"Body length: {responseString.Length}");

        var result = JsonSerializer.Deserialize<TournamentResponse>(responseString, JsonOptions);

        if (result?.Data == null)
            return new List<RawTournament>();

        return result.Data.Select(t => new RawTournament(
            t.Id,
            t.Name,
            t.City,
            t.Country,
            t.CountryCode,
            t.Date,
            t.EndDate,
            t.Category,
            t.Format
        )).ToList();
    }
}
