namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext : IContext
{
    public void Accept() => throw new NotImplementedException();

    public void Discard() => throw new NotImplementedException();

    public void Requeue() => throw new NotImplementedException();
}
