using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
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
            services.AddHostedService<InternalDataLoader>();
            services.AddSingleton<IPersitanceManager, MinioPersistanceManager>();
            services.AddSingleton<ITokenService, TokenService>();
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
                errorApp.Run(async context =>
                {
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    context.Response.ContentType = "text/json";

                    var exceptionHandlerPathFeature =
                        context.Features.Get<IExceptionHandlerPathFeature>();

                    if (exceptionHandlerPathFeature?.Error is CoflnetException ex)
                    {
                        context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        await context.Response.WriteAsync(
                                        JsonConvert.SerializeObject(new { ex.Slug, ex.Message }));

                    }
                    else
                    {
                        using var span = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("error").StartActive();
                        span.Span.Log(exceptionHandlerPathFeature?.Error?.Message);
                        span.Span.Log(exceptionHandlerPathFeature?.Error?.StackTrace);
                        var traceId = System.Net.Dns.GetHostName().Trim('-') + "." + span?.Span?.Context?.TraceId;
                        await context.Response.WriteAsync(
                            JsonConvert.SerializeObject(new
                            {
                                Slug = "internal_error",
                                Message = "An unexpected internal error occured. Please check that your request is valid. If it is please report he error and include the Trace.",
                                Trace = traceId
                            }));
                    }
                });
            });

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapMetrics();
                endpoints.MapControllers();
            });
        }
    }
}
