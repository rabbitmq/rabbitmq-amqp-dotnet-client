using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher(SenderLink senderLink) : IPublisher
{
    private readonly SenderLink _senderLink = senderLink;

    public async Task Publish(IMessage message)
    {
        using var nMessage = ((AmqpMessage)message).NativeMessage;
        await _senderLink.SendAsync(nMessage);
    }


    public State State { get; }

    public Task CloseAsync()
    {
        throw new NotImplementedException();
    }

    public event IClosable.LifeCycleCallBack? ChangeState;
}