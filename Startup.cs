using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
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

            services.AddControllers().AddNewtonsoftJson();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkySniper", Version = "v1" });
            });
            services.AddSingleton<SniperService>();
            services.AddSingleton<InternalDataLoader>();
            services.AddHostedService<InternalDataLoader>(d=>d.GetRequiredService<InternalDataLoader>());
            services.AddSingleton<ICraftsApi, CraftsApi>(d=>new CraftsApi(Configuration["CRAFTS_BASE_URl"]));
            services.AddSingleton<ICraftCostService, CraftCostService>();
            services.AddHostedService<CraftCostService>(d=>d.GetRequiredService<ICraftCostService>()  as CraftCostService);
            services.AddSingleton<IPersitanceManager, MinioPersistanceManager>();
            services.AddSingleton<ITokenService, TokenService>();
            services.AddSingleton<ActiveUpdater>();
            services.AddSingleton<PartialCalcService>(c => new PartialCalcService(c.GetRequiredService<SniperService>().Lookups, c.GetRequiredService<ICraftCostService>()));
            services.AddSingleton<Kafka.KafkaCreator>();
            services.AddJaeger(Configuration);
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
