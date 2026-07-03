namespace EsportTournamentsApp.Models;

// API Modeli:
public record TournamentResponse(List<TournamentData> Data, Meta Meta);
public record TournamentData(
    string Id,
    string Name,
    string City,
    string Country,
    string CountryCode,
    string Date,
    string EndDate,
    string Category,
    bool FideRated,
    int Rounds,
    string Format
);
public record Meta(int Page, int Limit, int Total, bool HasMore);

// Aktor Modeli:
public record RawTournament(string Id, string Name, string City, string Country, string Date, string EndDate, string Category, string Format);
public record TournamentDto(string Id, string Name, string City, string Country, string Date, string EndDate, string Category, string Format, string Status);
public record GroupedTournaments(string Category, List<TournamentDto> Upcoming, List<TournamentDto> Active, List<TournamentDto> Finished, int TotalCount, string LastUpdated);

// Poruke:
public record StartPeriodicFetch(string Category, TimeSpan Interval);
public record GetCurrentStateRequest(string Category);
public record ProcessingFailed(Exception Exception);
