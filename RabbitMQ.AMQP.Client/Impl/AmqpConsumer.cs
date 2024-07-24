using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumer : AbstractLifeCycle, IConsumer
{
    private readonly AmqpConnection _connection;
    private readonly string _address;
    private readonly MessageHandler _messageHandler;
    private readonly int _initialCredits = 0;
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
        OpenAsync();
        _connection.Consumers.TryAdd(Id, this);
    }


    protected sealed override Task OpenAsync()
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
                throw new ConsumerException("Failed to create receiver link. Link state is not attached, error: " +
                    _receiverLink.Error?.ToString() ?? "Unknown error");
            }

            OnNewStatus(State.Open, null);
            ProcessMessages();
        }
        catch (Exception e)
        {
            throw new ConsumerException($"Failed to create receiver link, {e}");
        }

        return Task.CompletedTask;
    }

    private void ProcessMessages()
    {
        // TODO: Check the performance during the download messages
        // The publisher is faster than the consumer
        _receiverLink?.Start(_initialCredits,
            (link, message) =>
            {
                IContext context = new DeliveryContext(link, message);
                _messageHandler(context,
                    new AmqpMessage(message));
            });
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


    internal void ChangeStatus(State newState, Error? error)
    {
        OnNewStatus(newState, error);
    }

    internal async Task Reconnect()
    {
        int randomWait = Random.Shared.Next(200, 800);
        Trace.WriteLine(TraceLevel.Information, $"Consumer: {ToString()} is reconnecting in {randomWait} ms");
        await Task.Delay(randomWait).ConfigureAwait(false);

        if (_receiverLink != null)
        {
            await _receiverLink.DetachAsync().ConfigureAwait(false)!;
        }

        await OpenAsync().ConfigureAwait(false);
    }
}
