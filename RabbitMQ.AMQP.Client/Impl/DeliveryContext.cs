using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext(IReceiverLink link, Message message) : IContext
{
    public Task Accept()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }
        link.Accept(message);
        message.Dispose();
        return Task.CompletedTask;
    }

    public Task Discard()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Reject(message);
        message.Dispose();
        return Task.CompletedTask;
    }

    public Task Requeue()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Release(message);
        message.Dispose();
        return Task.CompletedTask;
    }
}
