using System.Net;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;
using EsportTournamentsApp.Actors;
using EsportTournamentsApp.Models;

namespace EsportTournamentsApp;

public class Server
{
    private ActorSystem? _actorSystem;
    private IActorRef? _tournamentCoordinator;
    private HttpListener? _listener;
    private bool _isRunning = true;

    private async Task ProcessRequest(HttpListenerContext context)
    {
        try
        {
            await HandleRequest(context);
        }
        catch (Exception ex)
        {
            Logger.Log("SERVER ERROR", $"Greska pri obradi zahteva: {ex}");
        }
    }

    public async Task StartAsync()
    {
        var configText = File.ReadAllText("akka.conf");
        var config = ConfigurationFactory.ParseString(configText);

        _actorSystem = ActorSystem.Create("TournamentSystem", config);
        _tournamentCoordinator = _actorSystem.ActorOf(TournamentCoordinatorActor.Props(), "tournament-coordinator");

        _listener = new HttpListener();
        _listener.Prefixes.Add("http://localhost:5000/");
        _listener.Start();
        Logger.Log("SERVER", "Server slusa na adresi: http://localhost:5000/tournaments/{kategorija}");
        Logger.Log("SERVER", "Podrzane kategorije: Classical, Rapid, Blitz");
        Logger.Log("SERVER", "Primeri:");
        Logger.Log("SERVER", "  GET /tournaments/Classical");
        Logger.Log("SERVER", "  GET /tournaments/Blitz");
        Logger.Log("SERVER", "  GET /tournaments/Rapid");

        while (_isRunning)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = ProcessRequest(context);
            }
            catch (HttpListenerException) when (!_isRunning)
            {
            }
        }
    }

    private async Task HandleRequest(HttpListenerContext context)
    {
        var request = context.Request;
        Logger.Log("HTTP", $"{request.HttpMethod} {request.RawUrl}");
        var response = context.Response;

        if (request.RawUrl == "/favicon.ico")
        {
            response.StatusCode = 404;
            response.Close();
            return;
        }

        var pathParts = request.Url?.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (pathParts?.Length == 2 && pathParts[0] == "tournaments")
        {
            var category = Uri.UnescapeDataString(pathParts[1]);

            string[] validCategories = { "Classical", "Rapid", "Blitz" };
            if (!validCategories.Contains(category, StringComparer.OrdinalIgnoreCase))
            {
                response.StatusCode = 400;
                var errorJson = JsonSerializer.Serialize(new { error = $"Nepoznata kategorija: '{category}'. Podrzane: Classical, Rapid, Blitz." });
                var errorBuffer = Encoding.UTF8.GetBytes(errorJson);
                response.ContentType = "application/json";
                response.ContentLength64 = errorBuffer.Length;
                response.OutputStream.Write(errorBuffer, 0, errorBuffer.Length);
                Logger.Log("HTTP", $"Zahtev '{category}' - 400 Bad Request.");
                response.Close();
                return;
            }

            try
            {
                var query = request.QueryString;
                var country = query["country"];
                var format = query["format"];

                var result = await _tournamentCoordinator.Ask<GroupedTournaments>(
                    new GetCurrentStateRequest(category, country, format),
                    TimeSpan.FromSeconds(30)
                );

                var json = JsonSerializer.Serialize(result);
                byte[] buffer = Encoding.UTF8.GetBytes(json);
                response.ContentType = "application/json";
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);

                Logger.Log("HTTP", $"Zahtev '{category}' (country={country ?? "any"}, format={format ?? "any"}) uspesno obradjen (turnira: {result.TotalCount}).");
            }
            catch (TimeoutException)
            {
                response.StatusCode = 504;
                Logger.Log("HTTP", $"Zahtev '{category}' istekao (timeout).");
            }
            catch (Exception e)
            {
                Logger.Log("HTTP", $"Zahtev '{category}' neuspesan.");
                Logger.Log("SERVER ERROR", $"Izuzetak: {e}");
                response.StatusCode = 500;
            }
        }
        else
        {
            response.StatusCode = 404;
            var notFoundJson = JsonSerializer.Serialize(new { error = "Nepoznata ruta. Koristite: /tournaments/{kategorija}" });
            var notFoundBuffer = Encoding.UTF8.GetBytes(notFoundJson);
            response.ContentType = "application/json";
            response.ContentLength64 = notFoundBuffer.Length;
            response.OutputStream.Write(notFoundBuffer, 0, notFoundBuffer.Length);
        }
        response.Close();
    }

    public async Task StopAsync()
    {
        Logger.Log("SHUTDOWN", "Graceful Shutdown u toku...");

        _isRunning = false;
        _listener?.Stop();

        if (_actorSystem != null)
        {
            await _actorSystem.Terminate();
        }

        Logger.Log("SHUTDOWN", "Akka.NET sistem i HTTP server su uspesno ugaseni.");
    }
}
