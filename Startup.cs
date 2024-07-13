using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Crafts.Client.Api;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;
using Prometheus;
using StackExchange.Redis;

namespace Coflnet.Sky.Sniper
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers().AddNewtonsoftJson(options =>
            {
                options.SerializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            });
            services.AddSwaggerGenNewtonsoftSupport();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkySniper", Version = "v1" });
            });
            services.AddSingleton<SniperService>();
            services.AddSingleton<InternalDataLoader>();
            services.AddHostedService<InternalDataLoader>(d => d.GetRequiredService<InternalDataLoader>());
            services.AddSingleton<ICraftsApi, CraftsApi>(d => new CraftsApi(Configuration["CRAFTS_BASE_URL"]));
            services.AddSingleton<ICraftCostService, CraftCostService>();
            services.AddHostedService<CraftCostService>(d => d.GetRequiredService<ICraftCostService>() as CraftCostService);
            services.AddSingleton<Mayor.Client.Api.IMayorApi, Mayor.Client.Api.MayorApi>(d => new Mayor.Client.Api.MayorApi(Configuration["MAYOR_BASE_URL"]));
            services.AddSingleton<Mayor.Client.Api.IElectionPeriodsApi>(d => new Mayor.Client.Api.ElectionPeriodsApi(Configuration["MAYOR_BASE_URL"]));
            services.AddSingleton<IMayorService, MayorService>();
            services.AddHostedService<MayorService>(d => d.GetRequiredService<IMayorService>() as MayorService);
            services.AddSingleton<RetrainService>();
            services.AddHostedService<RetrainService>(d => d.GetRequiredService<RetrainService>());
            // register redis connectionmultiplexer
            services.AddSingleton<IConnectionMultiplexer>(d =>
            {
                var redisConfig = ConfigurationOptions.Parse(Configuration["REDIS_HOST"]);
                redisConfig.SyncTimeout = 5000;
                return ConnectionMultiplexer.Connect(redisConfig);
            });
            services.AddSingleton<IPersitanceManager, S3PersistanceManager>();
            services.AddSingleton<ITokenService, TokenService>();
            services.AddSingleton<ActiveUpdater>();
            services.AddSingleton<PartialCalcService>();
            services.AddSingleton<Kafka.KafkaCreator>();
            services.AddSingleton<HypixelItemService>();
            services.AddSingleton<IAttributeFlipService, AttributeFlipService>();
            services.AddSingleton<System.Net.Http.HttpClient>();
            services.AddJaeger(Configuration);
            services.AddTransient<HypixelContext>(di => new HypixelContext());
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "SkySniper v1");
                c.RoutePrefix = "api";
            });

            app.UseResponseCaching();

            app.UseRouting();

            app.UseAuthorization();

            app.UseExceptionHandler(errorApp =>
            {
                ErrorHandler.Add(errorApp.ApplicationServices.GetService<ILogger<Startup>>(), errorApp, "sniper");
            });

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapMetrics();
                endpoints.MapControllers();
            });
        }
    }
}
