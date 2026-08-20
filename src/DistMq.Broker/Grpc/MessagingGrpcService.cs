using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using Grpc.Core;

namespace DistMq.Broker.Grpc;

/// <summary>
/// gRPC adapter over <see cref="BrokerService"/>. It translates wire types and maps
/// broker errors onto status codes; all semantics live in the broker service.
/// </summary>
public sealed class MessagingGrpcService(BrokerService broker) : Messaging.MessagingBase
{
    public override async Task<SendResponse> Send(SendRequest request, ServerCallContext context)
    {
        var response = new SendResponse();
        try
        {
            var sequenceNumbers = await broker.SendAsync(
                ParsePath(request.Entity), request.Messages, context.CancellationToken);

            response.SequenceNumbers.AddRange(sequenceNumbers);
            return response;
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<ReceiveResponse> Receive(ReceiveRequest request, ServerCallContext context)
    {
        try
        {
            var messages = await broker.ReceiveAsync(
                ParsePath(request.Entity),
                request.MaxMessages <= 0 ? 1 : request.MaxMessages,
                request.Mode == ReceiveMode.Unspecified ? ReceiveMode.PeekLock : request.Mode,
                string.IsNullOrEmpty(request.ReceiverId) ? context.Peer : request.ReceiverId,
                TimeSpan.FromMilliseconds(Math.Max(0, request.MaxWaitMs)),
                context.CancellationToken);

            var response = new ReceiveResponse();
            response.Messages.AddRange(messages);
            return response;
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<SettleResponse> Settle(SettleRequest request, ServerCallContext context)
    {
        try
        {
            var results = await broker.SettleAsync(
                ParsePath(request.Entity), request.Action, request.Settlements, context.CancellationToken);

            var response = new SettleResponse();
            response.Results.AddRange(results);
            return response;
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<RenewLockResponse> RenewLock(RenewLockRequest request, ServerCallContext context)
    {
        try
        {
            var lockedUntil = await broker.RenewLockAsync(
                ParsePath(request.Entity), request.SequenceNumber, request.LockToken, context.CancellationToken);

            return new RenewLockResponse { LockedUntilTicks = lockedUntil.UtcTicks };
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<PeekResponse> Peek(PeekRequest request, ServerCallContext context)
    {
        try
        {
            var messages = await broker.PeekAsync(
                ParsePath(request.Entity),
                request.FromSequenceNumber,
                request.MaxMessages <= 0 ? 1 : request.MaxMessages,
                context.CancellationToken);

            var response = new PeekResponse();
            response.Messages.AddRange(messages);
            return response;
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<ScheduleMessageResponse> ScheduleMessage(
        ScheduleMessageRequest request, ServerCallContext context)
    {
        try
        {
            var sequenceNumber = await broker.ScheduleMessageAsync(
                ParsePath(request.Entity),
                request.Message,
                new DateTimeOffset(request.DueAtTicks, TimeSpan.Zero),
                context.CancellationToken);

            return new ScheduleMessageResponse { SequenceNumber = sequenceNumber };
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<CancelScheduledMessageResponse> CancelScheduledMessage(
        CancelScheduledMessageRequest request, ServerCallContext context)
    {
        try
        {
            var cancelled = await broker.CancelScheduledMessageAsync(
                ParsePath(request.Entity), request.SequenceNumber, context.CancellationToken);

            return new CancelScheduledMessageResponse { Cancelled = cancelled };
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    public override async Task<ReceiveDeferredResponse> ReceiveDeferred(
        ReceiveDeferredRequest request, ServerCallContext context)
    {
        try
        {
            var messages = await broker.ReceiveDeferredAsync(
                ParsePath(request.Entity),
                request.SequenceNumbers,
                string.IsNullOrEmpty(request.ReceiverId) ? context.Peer : request.ReceiverId,
                context.CancellationToken);

            var response = new ReceiveDeferredResponse();
            response.Messages.AddRange(messages);
            return response;
        }
        catch (DistMqException ex)
        {
            throw ToRpc(ex);
        }
    }

    private static EntityPath ParsePath(string entity)
    {
        if (!EntityPath.TryParse(entity, out var path))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"'{entity}' is not a valid entity path."));
        }

        return path;
    }

    /// <summary>
    /// Carries the broker's error code in trailers so a client can react to
    /// <c>LockLost</c> or <c>NotOwner</c> specifically rather than parsing a message.
    /// </summary>
    private static RpcException ToRpc(DistMqException exception)
    {
        var status = exception.Code switch
        {
            DistMqErrorCode.EntityNotFound => StatusCode.NotFound,
            DistMqErrorCode.EntityAlreadyExists => StatusCode.AlreadyExists,
            DistMqErrorCode.InvalidArgument => StatusCode.InvalidArgument,
            DistMqErrorCode.NotOwner => StatusCode.FailedPrecondition,
            DistMqErrorCode.LockLost or DistMqErrorCode.SessionLockLost => StatusCode.FailedPrecondition,
            DistMqErrorCode.SessionCannotBeLocked => StatusCode.FailedPrecondition,
            DistMqErrorCode.MessageSizeExceeded => StatusCode.InvalidArgument,
            DistMqErrorCode.MessageNotDeferred => StatusCode.FailedPrecondition,
            DistMqErrorCode.SessionRequirementMismatch => StatusCode.InvalidArgument,
            DistMqErrorCode.Throttled => StatusCode.ResourceExhausted,
            DistMqErrorCode.Fenced => StatusCode.Unavailable,
            _ => StatusCode.Internal,
        };

        var trailers = new Metadata { { "distmq-error-code", exception.Code.ToString() } };
        if (exception.RedirectEndpoint is { } endpoint)
        {
            trailers.Add("distmq-redirect", endpoint);
        }

        return new RpcException(new Status(status, exception.Message), trailers);
    }
}
