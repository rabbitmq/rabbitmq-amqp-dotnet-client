using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher : AbstractClosable, IPublisher
{
    private readonly SenderLink _senderLink;
    private readonly ManualResetEvent _stopPublishing = new(true);
    private readonly AmqpConnection _connection;

    public AmqpPublisher(AmqpConnection connection, string address)
    {
        _connection = connection;
        // TODO: Is it correct  the Id here?
        _senderLink = new SenderLink(_connection.GetNativeSession(), Id, address);
        connection.Publishers.TryAdd(Id, this);
        State = State.Open;
    }

    public string Id { get; } = Guid.NewGuid().ToString();


    public void PausePublishing()
    {
        _stopPublishing.Reset();
    }

    public void ResumePublishing()
    {
        _stopPublishing.Set();
    }

    public async Task Publish(IMessage message)
    {
        ThrowIfClosed();
        try
        {
            _stopPublishing.WaitOne();
            using var nMessage = ((AmqpMessage)message).NativeMessage;
            await _senderLink.SendAsync(nMessage);
        }
        catch (Exception e)
        {
            throw new PublisherException("Failed to publish message", e);
        }
    }


    public override async Task CloseAsync()
    {
        OnNewStatus(State.Closing, null);
        _connection.Publishers.TryRemove(Id, out _);

        try
        {
            await _senderLink.CloseAsync();
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway", e);
        }

        OnNewStatus(State.Closed, null);
    }
}