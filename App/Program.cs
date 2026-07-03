

namespace EsportTournamentsApp
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var server = new Server();

            Console.CancelKeyPress += async (sender, e) =>
            {
                e.Cancel = true;
                await server.StopAsync();
            };

            await server.StartAsync();
        }
    }
}
