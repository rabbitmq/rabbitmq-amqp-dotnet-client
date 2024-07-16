using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext(IReceiverLink link, Message message) : IContext
{
    public void Accept()
    {
        link.Accept(message);
    }

    public void Discard()
    {
        link.Reject(message);
    }

    public void Requeue()
    {
        link.Release(message);
    }
}
