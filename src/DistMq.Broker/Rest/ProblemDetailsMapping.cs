using DistMq.Core;
using Microsoft.AspNetCore.Diagnostics;

namespace DistMq.Broker.Rest;

/// <summary>
/// Maps <see cref="DistMqException"/> onto HTTP status codes, so REST clients see the
/// same error vocabulary as gRPC clients (ADR 0010).
/// </summary>
public static class ProblemDetailsMapping
{
    public static IApplicationBuilder UseDistMqProblemDetails(this IApplicationBuilder app) =>
        app.UseExceptionHandler(handler => handler.Run(async context =>
        {
            var exception = context.Features.Get<IExceptionHandlerFeature>()?.Error;
            if (exception is not DistMqException distMq)
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                await context.Response.WriteAsJsonAsync(new { error = "Unknown", message = "An unexpected error occurred." });
                return;
            }

            context.Response.StatusCode = StatusFor(distMq.Code);
            if (distMq.RetryAfter is { } retryAfter)
            {
                context.Response.Headers.RetryAfter = ((int)retryAfter.TotalSeconds).ToString();
            }

            await context.Response.WriteAsJsonAsync(new
            {
                error = distMq.Code.ToString(),
                message = distMq.Message,
                redirect = distMq.RedirectEndpoint,
            });
        }));

    private static int StatusFor(DistMqErrorCode code) => code switch
    {
        DistMqErrorCode.EntityNotFound => StatusCodes.Status404NotFound,
        DistMqErrorCode.EntityAlreadyExists => StatusCodes.Status409Conflict,
        DistMqErrorCode.InvalidArgument => StatusCodes.Status400BadRequest,
        DistMqErrorCode.MessageSizeExceeded => StatusCodes.Status413PayloadTooLarge,
        DistMqErrorCode.SessionRequirementMismatch => StatusCodes.Status400BadRequest,
        DistMqErrorCode.LockLost => StatusCodes.Status409Conflict,
        DistMqErrorCode.SessionLockLost => StatusCodes.Status409Conflict,
        DistMqErrorCode.SessionCannotBeLocked => StatusCodes.Status409Conflict,
        DistMqErrorCode.MessageNotDeferred => StatusCodes.Status409Conflict,
        DistMqErrorCode.NotOwner => StatusCodes.Status421MisdirectedRequest,
        DistMqErrorCode.Throttled => StatusCodes.Status429TooManyRequests,
        DistMqErrorCode.Fenced => StatusCodes.Status503ServiceUnavailable,
        _ => StatusCodes.Status500InternalServerError,
    };
}
