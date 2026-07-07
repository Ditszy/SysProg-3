using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;
using EsportTournamentsApp.Models;

namespace EsportTournamentsApp.Actors;

public class TournamentCoordinatorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly ConcurrentDictionary<string, DateTime> _lastAccessTimes = new();
    private IDisposable? _ttlSubscription;

    private static readonly TimeSpan TtlThreshold = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan CheckInterval = TimeSpan.FromMinutes(1);

    public TournamentCoordinatorActor()
    {
        StartTtlChecker();

        Receive<GetCurrentStateRequest>(request =>
        {
            var actorName = BuildKey(request.Category, request.Country, request.Format);

            _lastAccessTimes[actorName] = DateTime.UtcNow;

            var child = Context.Child(actorName);
            if (child is Nobody)
            {
                child = Context.ActorOf(
                    TournamentManagerActor.Props(request.Category, request.Country, request.Format)
                        .WithDispatcher("akka.actor.tournament-dispatcher"),
                    actorName
                );

                Context.Watch(child);

                _log.Info($"Child aktor kreiran: {actorName}");

                child.Tell(new StartPeriodicFetch(request.Category, request.Country, request.Format, TimeSpan.FromSeconds(60)));

                child.Forward(request);
            }
            else
            {
                child.Forward(request);
            }
        });

        Receive<Terminated>(terminated =>
        {
            var childName = terminated.ActorRef.Path.Name;
            _lastAccessTimes.TryRemove(childName, out _);
            _log.Info($"Child aktor terminisan: {childName}");
        });
    }

    private void StartTtlChecker()
    {
        var self = Self;
        _ttlSubscription = Observable.Interval(CheckInterval, TaskPoolScheduler.Default)
            .Subscribe(_ => self.Tell(new CheckExpiredChildren()));

        Receive<CheckExpiredChildren>(_ =>
        {
            var now = DateTime.UtcNow;
            var expired = _lastAccessTimes
                .Where(kvp => now - kvp.Value > TtlThreshold)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in expired)
            {
                var child = Context.Child(key);
                if (child is not Nobody)
                {
                    _log.Info($"TTL istekao ({TtlThreshold.TotalMinutes}min) - saljem StopTournamentActor: {key}");
                    child.Tell(new StopTournamentActor());
                }
            }
        });
    }

    protected override void PostStop()
    {
        _ttlSubscription?.Dispose();
        base.PostStop();
    }

    private static string BuildKey(string category, string? country, string? format)
    {
        return $"{category}_{country ?? "any"}_{format ?? "any"}".ToLowerInvariant();
    }

    public static Props Props() => Akka.Actor.Props.Create<TournamentCoordinatorActor>();

    private record CheckExpiredChildren;
}
