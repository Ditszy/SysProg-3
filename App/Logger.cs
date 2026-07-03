
namespace EsportTournamentsApp
{
    public static class Logger
    {
        public static void Log(string tag, string message)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{tag}] {message}");
        }
    }
}
