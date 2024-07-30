using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumer : AbstractReconnectLifeCycle, IConsumer
{
    private readonly AmqpConnection _connection;
    private readonly string _address;
    private readonly MessageHandler _messageHandler;
    private readonly int _initialCredits;
    private readonly Map _filters;
    private ReceiverLink? _receiverLink;

    public AmqpConsumer(AmqpConnection connection, string address,
        MessageHandler messageHandler, int initialCredits, Map filters)
    {
        _connection = connection;
        _address = address;
        _messageHandler = messageHandler;
        _initialCredits = initialCredits;
        _filters = filters;
        _ = OpenAsync();
        _connection.Consumers.TryAdd(Id, this);
    }


    protected sealed override async Task OpenAsync()
    {
        try
        {
            var attachCompleted = new ManualResetEvent(false);
            _receiverLink = new ReceiverLink(_connection._nativePubSubSessions.GetOrCreateSession(), Id,
                Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, Id, _filters),
                (link, attach) => { attachCompleted.Set(); });

            attachCompleted.WaitOne(TimeSpan.FromSeconds(5));
            if (_receiverLink.LinkState != LinkState.Attached)
            {
                throw new ConsumerException(
                    $"{ToString()} Failed to create receiver link. Link state is not attached, error: " +
                    _receiverLink.Error?.ToString() ?? "Unknown error");
            }

            OnNewStatus(State.Open, null);
            await ProcessMessages().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            throw new ConsumerException($"{ToString()} Failed to create receiver link, {e}");
        }
    }

    private async Task ProcessMessages()
    {
        try
        {
            if (_receiverLink == null)
            {
                return;
            }

            _receiverLink.SetCredit(_initialCredits);
            while (_receiverLink is { LinkState: LinkState.Attached })
            {
                TimeSpan timeout = TimeSpan.FromSeconds(59);
                Message message = await _receiverLink.ReceiveAsync(timeout).ConfigureAwait(false);
                if (message == null)
                {
                    // this is not a problem, it is just a timeout. 
                    // the timeout is set to 60 seconds. 
                    // For the moment I'd trace it at some point we can remove it
                    Trace.WriteLine(TraceLevel.Verbose,
                        $"{ToString()}: Timeout {timeout.Seconds} s.. waiting for message.");
                    continue;
                }

                IContext context = new DeliveryContext(_receiverLink, message);
                await _messageHandler(context,
                    new AmqpMessage(message)).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            if (State == State.Closing)
            {
                return;
            }

            Trace.WriteLine(TraceLevel.Error, $"{ToString()} Failed to process messages, {e}");
        }


        Trace.WriteLine(TraceLevel.Verbose, $"{ToString()} is closed.");
    }

    private string Id { get; } = Guid.NewGuid().ToString();

    public void Pause()
    {
        throw new System.NotImplementedException();
    }

    public long UnsettledMessageCount()
    {
        throw new System.NotImplementedException();
    }

    public void Unpause()
    {
        throw new System.NotImplementedException();
    }


    public override async Task CloseAsync()
    {
        if (_receiverLink == null)
        {
            return;
        }

        OnNewStatus(State.Closing, null);
        await (_receiverLink.CloseAsync()).ConfigureAwait(false);
        _receiverLink = null;
        OnNewStatus(State.Closed, null);
        _connection.Consumers.TryRemove(Id, out _);
    }

    public override string ToString()
    {
        return $"Consumer{{Address='{_address}', " +
               $"id={Id}, " +
               $"ConnectionName='{_connection}', " +
               $"State='{State}'}}";
    }
}
