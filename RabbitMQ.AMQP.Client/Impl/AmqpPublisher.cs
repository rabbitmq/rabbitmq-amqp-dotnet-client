using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher(AmqpConnection connection, string address) : IPublisher
{
    public string Id { get; } = Guid.NewGuid().ToString();

    private readonly SenderLink _senderLink = new(connection.GetNativeSession(), "sender-link", address);


    public async Task Publish(IMessage message)
    {
        try
        {
            using var nMessage = ((AmqpMessage)message).NativeMessage;
            await _senderLink.SendAsync(nMessage);
        }
        catch (Exception e)
        {
            throw new PublisherException("Failed to publish message", e);
        }
    }


    public State State { get; }

    public Task CloseAsync()
    {
        ChangeState?.Invoke(this, State.Closing, State.Closed, null);
        connection.Publishers.TryRemove(Id, out _);

        return Task.CompletedTask;
    }

    public event IClosable.LifeCycleCallBack? ChangeState;
}