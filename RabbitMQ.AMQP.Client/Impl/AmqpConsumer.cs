using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumer : AbstractResourceStatus, IConsumer
{
    private readonly AmqpConnection _connection;
    private readonly string _address;
    private readonly MessageHandler _messageHandler;
    private readonly int _initialCredits = 0;
    private readonly IContext _context;
    private ReceiverLink? _receiverLink;

    public AmqpConsumer(AmqpConnection connection, string address, MessageHandler messageHandler, int initialCredits)
    {
        _connection = connection;
        _address = address;
        _messageHandler = messageHandler;
        _initialCredits = initialCredits;
        _context = new DeliveryContext();
        Connect();
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
                _messageHandler(_context, new AmqpMessage(message));
                link.Accept(message);
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


    public Task CloseAsync() => throw new NotImplementedException();
}
