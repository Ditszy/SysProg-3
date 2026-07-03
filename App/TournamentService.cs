using System.Text.Json;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using EsportTournamentsApp.Models;

namespace EsportTournamentsApp.Services;

public class TournamentService
{
    private readonly HttpClient _httpClient;
    private const string BaseUrl = "https://tourneyradar-api.vercel.app/v1/tournaments";

    public TournamentService(HttpClient httpClient) => _httpClient = httpClient;

    public IObservable<List<RawTournament>> WatchTournaments(string category, TimeSpan interval)
    {
        return Observable.Interval(interval, TaskPoolScheduler.Default)
            .StartWith(0L)
            .SelectMany(_ =>
                Observable.FromAsync(() => Fetch(category))
                    .Catch<List<RawTournament>, Exception>(ex =>
                    {
                        Logger.Log("API ERROR", $"Fetch za '{category}' neuspesan: {ex.Message}");
                        return Observable.Return(new List<RawTournament>());
                    }));
    }

    private async Task<List<RawTournament>> Fetch(string category)
    {
        var url = $"{BaseUrl}?category={category}&limit=100";

        Logger.Log("API CALL", $"Nit {Environment.CurrentManagedThreadId} povlaci turnire za kategoriju '{category}'...");
        Logger.Log("API CALL", $"URL: {url}");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Logger.Log("API CALL", $"Response status: {response.StatusCode}");
        response.EnsureSuccessStatusCode();
        var responseString = await response.Content.ReadAsStringAsync(cts.Token);
        Logger.Log("API CALL", $"Body length: {responseString.Length}");
        var json = JsonDocument.Parse(responseString);

        if (!json.RootElement.TryGetProperty("data", out var dataProp) ||
            dataProp.ValueKind != JsonValueKind.Array)
        {
            return new List<RawTournament>();
        }

        var tournaments = new List<RawTournament>();

        foreach (var item in dataProp.EnumerateArray())
        {
            tournaments.Add(new RawTournament(
                item.TryGetProperty("id", out var id) ? id.GetString() ?? "" : "",
                item.TryGetProperty("name", out var name) ? name.GetString() ?? "Nepoznat" : "Nepoznat",
                item.TryGetProperty("city", out var city) ? city.GetString() ?? "" : "",
                item.TryGetProperty("country", out var country) ? country.GetString() ?? "" : "",
                item.TryGetProperty("date", out var date) ? date.GetString() ?? "" : "",
                item.TryGetProperty("end_date", out var endDate) ? endDate.GetString() ?? "" : "",
                item.TryGetProperty("category", out var cat) ? cat.GetString() ?? category : category,
                item.TryGetProperty("format", out var format) ? format.GetString() ?? "" : ""
            ));
        }

        return tournaments;
    }
}
