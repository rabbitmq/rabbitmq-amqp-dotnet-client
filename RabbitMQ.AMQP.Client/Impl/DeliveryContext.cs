using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext(IReceiverLink link, Message message) : IContext
{
    public void Accept()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Accept(message);
    }

    public void Discard()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Reject(message);
    }

    public void Requeue()
    {
        if (!link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Release(message);
    }
}
