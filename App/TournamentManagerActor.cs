using Akka.Actor;
using Akka.Event;
using EsportTournamentsApp.Models;
using EsportTournamentsApp.Services;

namespace EsportTournamentsApp.Actors;

public class TournamentManagerActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly TournamentService _tournamentService = new(new HttpClient { Timeout = TimeSpan.FromSeconds(60) });

    private readonly string _currentCategory;
    private readonly string? _currentCountry;
    private readonly string? _currentFormat;
    private List<TournamentDto> _cachedUpcoming = new();
    private List<TournamentDto> _cachedActive = new();
    private List<TournamentDto> _cachedFinished = new();
    private int _totalCount = 0;
    private string _lastUpdated = "";

    private bool _isDataInitialized = false;

    private IDisposable? _rxSubscription;

    private readonly List<IActorRef> _waitingHttpRequesters = new();
    public TournamentManagerActor(string category, string? country, string? format)
    {
        _currentCategory = category;
        _currentCountry = country;
        _currentFormat = format;
        _log.Info($"Konstruktor TournamentManagerActor: {category} | country={country ?? "any"} | format={format ?? "any"}");

        Receive<StartPeriodicFetch>(start =>
        {
            if (_rxSubscription != null) return;

            _log.Info($"[TAJMER] Rx pokrenut za: {_currentCategory}");

            var self = Self;

            _rxSubscription = _tournamentService.WatchTournaments(_currentCategory, _currentCountry, _currentFormat, start.Interval)
                   .Subscribe(
                        tournaments =>
                        {
                            self.Tell(new TournamentsFetched(tournaments));
                        },
                        error =>
                        {
                            _log.Error(error, $"[RX ERROR] Greska u Observable za kategoriju: {_currentCategory}");
                            self.Tell(new ProcessingFailed(error));
                        });
        });

        Receive<GetCurrentStateRequest>(_ =>
        {
            if (_isDataInitialized)
            {
                Sender.Tell(BuildResult());
                return;
            }
            _waitingHttpRequesters.Add(Sender);
            _log.Info($"[AKTOR] Zahtev za '{_currentCategory}' u redu cekanja. Cekalaca: {_waitingHttpRequesters.Count}");
        });

        Receive<TournamentsFetched>(msg =>
        {
            var tournaments = msg.Tournaments
                .Where(t => string.Equals(t.Category, _currentCategory, StringComparison.OrdinalIgnoreCase))
                .Where(t => _currentCountry == null || string.Equals(t.CountryCode, _currentCountry, StringComparison.OrdinalIgnoreCase))
                .Where(t => _currentFormat == null || string.Equals(t.Format, _currentFormat, StringComparison.OrdinalIgnoreCase))
                .ToList();
            _log.Info($"Broj turnira za '{_currentCategory}/{_currentCountry ?? "any"}/{_currentFormat ?? "any"}': {tournaments.Count} (od ukupno {msg.Tournaments.Count}).");

            var task = Task.Run(() =>
            {
                var now = DateTime.UtcNow;
                var upcoming = new List<TournamentDto>();
                var active = new List<TournamentDto>();
                var finished = new List<TournamentDto>();

                foreach (var t in tournaments)
                {
                    string status;
                    if (DateTime.TryParse(t.Date, out var startDate))
                    {
                        if (DateTime.TryParse(t.EndDate, out var endDate))
                        {
                            if (now < startDate) status = "Upcoming";
                            else if (now >= startDate && now <= endDate) status = "Active";
                            else status = "Finished";
                        }
                        else
                        {
                            status = now < startDate ? "Upcoming" : "Finished";
                        }
                    }
                    else
                    {
                        status = "Unknown";
                    }

                    var dto = new TournamentDto(t.Id, t.Name, t.City, t.Country, t.Date, t.EndDate, t.Category, t.Format, status);

                    switch (status)
                    {
                        case "Upcoming": upcoming.Add(dto); break;
                        case "Active": active.Add(dto); break;
                        default: finished.Add(dto); break;
                    }
                }

                return new GroupedTournaments(
                    _currentCategory,
                    upcoming.OrderByDescending(t => t.Date).ToList(),
                    active.OrderByDescending(t => t.Date).ToList(),
                    finished.OrderByDescending(t => t.Date).ToList(),
                    tournaments.Count,
                    DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + " UTC"
                );
            });

            task.PipeTo(
                Self,
                success: result => new ProcessedResultReady(result),
                failure: ex => new ProcessingFailed(ex)
            );
        });

        Receive<ProcessingFailed>(m =>
        {
            _log.Error(m.Exception, "Obrada neuspesna.");

            foreach (var requester in _waitingHttpRequesters)
                requester.Tell(new Status.Failure(m.Exception));

            _waitingHttpRequesters.Clear();
        });

        Receive<ProcessedResultReady>(msg =>
        {
            _cachedUpcoming = msg.Result.Upcoming;
            _cachedActive = msg.Result.Active;
            _cachedFinished = msg.Result.Finished;
            _totalCount = msg.Result.TotalCount;
            _lastUpdated = msg.Result.LastUpdated;
            _isDataInitialized = true;

            _log.Info($"Azuriran kes kategorije '{_currentCategory}': {_totalCount} turnira. Odgovaram {_waitingHttpRequesters.Count} cekalaca.");

            var result = BuildResult();

            foreach (var requester in _waitingHttpRequesters)
                requester.Tell(result);

            _waitingHttpRequesters.Clear();
        });
    }

    protected override void PostStop()
    {
        _log.Info($"PostStop: Dispose Rx subscription za '{_currentCategory}'.");
        _rxSubscription?.Dispose();
        base.PostStop();
    }

    private GroupedTournaments BuildResult() => new(
        _currentCategory,
        _cachedUpcoming,
        _cachedActive,
        _cachedFinished,
        _totalCount,
        _lastUpdated
    );

    public record TournamentsFetched(List<RawTournament> Tournaments);
    public record ProcessedResultReady(GroupedTournaments Result);

    public static Props Props(string category, string? country, string? format)
        => Akka.Actor.Props.Create(() => new TournamentManagerActor(category, country, format))
            .WithDispatcher("akka.actor.tournament-dispatcher");
}
