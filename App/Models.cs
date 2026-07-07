using System.Text.Json.Serialization;

namespace EsportTournamentsApp.Models;

// API Modeli:
public record TournamentResponse(List<TournamentData> Data);
public record TournamentData(
    string Id,
    string Name,
    string City,
    string Country,
    [property: JsonPropertyName("country_code")] string CountryCode,
    string Date,
    [property: JsonPropertyName("end_date")] string EndDate,
    string Category,
    [property: JsonPropertyName("fide_rated")] bool FideRated,
    int Rounds,
    string Format
);

// Aktor Modeli:
public record RawTournament(string Id, string Name, string City, string Country, string CountryCode, string Date, string EndDate, string Category, string Format);
public record TournamentDto(string Id, string Name, string City, string Country, string Date, string EndDate, string Category, string Format, string Status);
public record GroupedTournaments(string Category, List<TournamentDto> Upcoming, List<TournamentDto> Active, List<TournamentDto> Finished, int TotalCount, string LastUpdated);

// Poruke:
public record StartPeriodicFetch(string Category, string? Country, string? Format, TimeSpan Interval);
public record GetCurrentStateRequest(string Category, string? Country, string? Format);
public record ProcessingFailed(Exception Exception);
public record StopTournamentActor;
