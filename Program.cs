using Coflnet.Core;
using Coflnet.Security.OpenBao;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper
{
    public class SnipProgram
    {
        public static void Main(string[] args)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
#if AI_FINDER
            if (AIFinder.IsolatedFastTreeTrainer.TryRun(args)) return;
            // the AI finder fits its models in a child process of this assembly
            AIFinder.IsolatedFastTreeTrainer.HostAssemblyPath = typeof(SnipProgram).Assembly.Location;
#endif
            var host = CreateHostBuilder(args).Build();
            LogAiFinderVariant(host.Services);
            HypixelContext.SetConfiguration(host.Services.GetRequiredService<IConfiguration>());
            host.Run();
        }

        /// <summary>One line at startup that says whether this build contains the AI finder, so a build without it is noticed</summary>
        private static void LogAiFinderVariant(System.IServiceProvider services)
        {
            var (active, variant) = Services.AiFinderRegistration.Describe(services.GetRequiredService<IConfiguration>());
            var logger = services.GetRequiredService<ILogger<SnipProgram>>();
            if (active)
                logger.LogInformation("AI finder: {Variant}", variant);
            else
                logger.LogWarning("AI finder: {Variant}, no AI flips will be found", variant);
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((_, config) => config.AddOpenBaoFromEnvironment())
                .ConfigureLogging((context, logging) =>
                {
                    // Shared OTel logging configuration from Coflnet.Core.
                    // Bridges ILogger -> OTLP (HttpProtobuf) with trace-log correlation,
                    // k8s pod attributes, and DEV_LOGGING console fallback.
                    logging.AddOpenTelemetryLogging(
                        context.Configuration,
                        context.Configuration["JAEGER_SERVICE_NAME"] ?? "sky-sniper");
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
