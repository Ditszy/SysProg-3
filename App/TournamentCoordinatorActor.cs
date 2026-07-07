using Akka.Actor;
using Akka.Event;
using EsportTournamentsApp.Models;

namespace EsportTournamentsApp.Actors;

public class TournamentCoordinatorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public TournamentCoordinatorActor()
    {
        Receive<GetCurrentStateRequest>(request =>
        {
            var actorName = BuildKey(request.Category, request.Country, request.Format);

            var child = Context.Child(actorName);
            if (child is Nobody)
            {
                child = Context.ActorOf(
                    TournamentManagerActor.Props(request.Category, request.Country, request.Format)
                        .WithDispatcher("akka.actor.tournament-dispatcher"),
                    actorName
                );

                _log.Info($"Child aktor kreiran: {actorName}");

                child.Tell(new StartPeriodicFetch(request.Category, request.Country, request.Format, TimeSpan.FromSeconds(60)));

                child.Forward(request);
            }
            else
            {
                child.Forward(request);
            }
        });
    }

    private static string BuildKey(string category, string? country, string? format)
    {
        return $"{category}_{country ?? "any"}_{format ?? "any"}".ToLowerInvariant();
    }

    public static Props Props() => Akka.Actor.Props.Create<TournamentCoordinatorActor>();
}
