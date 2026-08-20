using DistMq.Broker;
using DistMq.Broker.Grpc;
using DistMq.Broker.Observability;
using DistMq.Broker.Rest;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// Two listeners, because a cleartext port cannot serve both protocols: without TLS there
// is no ALPN, so Kestrel's Http1AndHttp2 falls back to HTTP/1.1 and every gRPC call fails
// with HTTP_1_1_REQUIRED. Behind TLS-terminating ingress one port would do; these ports are
// what the broker itself speaks.
//
//   :5000  HTTP/1.1  REST administration, the HTTP data plane, WebSocket bridge
//   :5001  HTTP/2    gRPC data plane
var httpPort = builder.Configuration.GetValue("DistMq:HttpPort", 5000);
var grpcPort = builder.Configuration.GetValue("DistMq:GrpcPort", 5001);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(httpPort, endpoint => endpoint.Protocols = HttpProtocols.Http1);
    kestrel.ListenAnyIP(grpcPort, endpoint => endpoint.Protocols = HttpProtocols.Http2);
});

builder.Services.AddDistMqBroker(builder.Configuration);

// Exported only when an OTLP endpoint is configured; otherwise the instruments are still
// live and can be scraped in-process, but nothing is shipped anywhere.
var otlpEndpoint = builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"];

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(
        serviceName: "distmq-broker",
        serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.1.0"))
    .WithTracing(tracing =>
    {
        tracing.AddSource(DistMqTelemetry.SourceName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation();

        if (!string.IsNullOrEmpty(otlpEndpoint))
        {
            tracing.AddOtlpExporter();
        }
    })
    .WithMetrics(metrics =>
    {
        metrics.AddMeter(DistMqTelemetry.SourceName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation();

        if (!string.IsNullOrEmpty(otlpEndpoint))
        {
            metrics.AddOtlpExporter();
        }
    });

var app = builder.Build();

await app.Services.InitializeDistMqStorageAsync();

app.UseDistMqProblemDetails();
app.UseWebSockets();

app.MapGrpcService<MessagingGrpcService>();
app.MapAdminEndpoints();
app.MapDataEndpoints();
app.MapWebSocketBridge();

app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", () => Results.Ok(new { status = "ready" }));

await app.RunAsync();

/// <summary>Exposed so the integration tests can host the broker with WebApplicationFactory.</summary>
public partial class Program;
