using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher : AbstractReconnectLifeCycle, IPublisher
{
    private readonly AmqpConnection _connection;
    private readonly TimeSpan _timeout;
    private readonly string _address;
    private readonly int _maxInFlight;

    private SenderLink? _senderLink = null;

    public AmqpPublisher(AmqpConnection connection, string address, TimeSpan timeout, int maxInFlight)
    {
        _address = address;
        _connection = connection;
        _timeout = timeout;
        _maxInFlight = maxInFlight;
        connection.Publishers.TryAdd(Id, this);
    }

    public override async Task OpenAsync()
    {
        try
        {
            TaskCompletionSource attachCompletedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            Attach attach = Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, Id);

            void onAttached(ILink argLink, Attach argAttach)
            {
                attachCompletedTcs.SetResult();
            }

            Task senderLinkTask = Task.Run(() =>
            {
                _senderLink = new SenderLink(_connection._nativePubSubSessions.GetOrCreateSession(), Id, attach, onAttached);
            });

            // TODO configurable timeout
            TimeSpan waitSpan = TimeSpan.FromSeconds(5);

            await attachCompletedTcs.Task.WaitAsync(waitSpan)
                .ConfigureAwait(false);

            await senderLinkTask.WaitAsync(waitSpan)
                .ConfigureAwait(false);

            if (_senderLink is null)
            {
                throw new PublisherException($"{ToString()} Failed to create sender link (null was returned)");
            }
            else if (_senderLink.LinkState != LinkState.Attached)
            {
                throw new PublisherException($"{ToString()} Failed to create sender link. Link state is not attached, error: " +
                    _senderLink.Error?.ToString() ?? "Unknown error");
            }
            else
            {
                await base.OpenAsync()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            throw new PublisherException($"{ToString()} Failed to create sender link, {e}");
        }
    }

    private string Id { get; } = Guid.NewGuid().ToString();

    // TODO: Consider implementing this method with the send method
    // a way to send a batch of messages

    // protected override async Task<int> ExecuteAsync(SenderLink link)
    // {
    //     int batch = this.random.Next(1, this.role.Args.Batch);
    //     Message[] messages = CreateMessages(this.id, this.total, batch);
    //     await Task.WhenAll(messages.Select(m => link.SendAsync(m)));
    //     this.total += batch;
    //     return batch;
    // }

    public async Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();

        if (_senderLink is null)
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_senderLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        try
        {
            Debug.Assert(false == _senderLink.IsClosed);
            Message nativeMessage = ((AmqpMessage)message).NativeMessage;
            await _senderLink.SendAsync(nativeMessage)
                .ConfigureAwait(false);

            var publishOutcome = new PublishOutcome(OutcomeState.Accepted, null);
            return new PublishResult(message, publishOutcome);
        }
        catch (AmqpException ex)
        {
            var publishOutcome = new PublishOutcome(OutcomeState.Failed, Utils.ConvertError(ex.Error));
            return new PublishResult(message, publishOutcome);
        }
        catch (Exception e)
        {
            throw new PublisherException($"{ToString()} Failed to publish message, {e}");
        }
    }

    public override async Task CloseAsync()
    {
        if (State == State.Closed)
        {
            return;
        }

        OnNewStatus(State.Closing, null);
        try
        {
            if (_senderLink != null)
            {
                await _senderLink.DetachAsync().ConfigureAwait(false);
                await _senderLink.CloseAsync()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway", e);
        }

        OnNewStatus(State.Closed, null);
        _connection.Publishers.TryRemove(Id, out _);
    }

    public override string ToString()
    {
        return $"Publisher{{Address='{_address}', id={Id} ConnectionName='{_connection}', State='{State}'}}";
    }
}
