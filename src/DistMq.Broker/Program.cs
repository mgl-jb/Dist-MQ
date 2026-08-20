using DistMq.Broker;
using DistMq.Broker.Grpc;
using DistMq.Broker.Rest;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDistMqBroker(builder.Configuration);

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
