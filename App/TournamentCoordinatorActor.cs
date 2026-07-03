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
            var actorName = request.Category.ToLower();

            var child = Context.Child(actorName);
            if (child is Nobody)
            {
                child = Context.ActorOf(
                    TournamentManagerActor.Props(request.Category)
                        .WithDispatcher("akka.actor.tournament-dispatcher"),
                    actorName
                );

                _log.Info($"Child aktor kreiran za kategoriju: {request.Category}");

                child.Tell(new StartPeriodicFetch(request.Category, TimeSpan.FromSeconds(60)));

                _log.Info($"StartPeriodicFetch izvrsen za: {request.Category}");

                child.Forward(request);
                _log.Info($"Forward uspesan za: {request.Category}");
            }
            else
            {
                child.Forward(request);
            }
        });
    }

    public static Props Props() => Akka.Actor.Props.Create<TournamentCoordinatorActor>();
}
