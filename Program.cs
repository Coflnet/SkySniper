using Coflnet.Security.OpenBao;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace Coflnet.Sky.Sniper
{
    public class SnipProgram
    {
        public static void Main(string[] args)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((_, config) => config.AddOpenBaoFromEnvironment())
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
