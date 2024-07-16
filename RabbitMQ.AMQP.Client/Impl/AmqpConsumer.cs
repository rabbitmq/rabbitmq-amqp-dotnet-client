using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumer : AbstractResourceStatus, IConsumer
{
    private readonly AmqpConnection _connection;
    private readonly string _address;
    private readonly MessageHandler _messageHandler;
    private readonly int _initialCredits = 0;
    private ReceiverLink? _receiverLink;

    public AmqpConsumer(AmqpConnection connection, string address, MessageHandler messageHandler, int initialCredits)
    {
        _connection = connection;
        _address = address;
        _messageHandler = messageHandler;
        _initialCredits = initialCredits;
        Connect();
        _connection.Consumers.TryAdd(Id, this); // TODO: Close all consumers on connection close
    }


    private void Connect()
    {
        try
        {
            var attachCompleted = new ManualResetEvent(false);
            _receiverLink = new ReceiverLink(_connection.NativePubSubSessions.GetOrCreateSession(), Id,
                Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, Id),
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
    }

    private void ProcessMessages()
    {
        _receiverLink?.Start(_initialCredits,
            (link, message) =>
            {
                IContext context = new DeliveryContext(link, message);
                _messageHandler(context,
                    new AmqpMessage(message));
            });
    }

    public string Id { get; } = Guid.NewGuid().ToString();

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


    public async Task CloseAsync()
    {
        if (_receiverLink == null)
        {
            return;
        }
        await (_receiverLink.CloseAsync()).ConfigureAwait(false);
    }
}
