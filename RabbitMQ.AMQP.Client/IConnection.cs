using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client;

public class ConnectionException : Exception
{
    public ConnectionException(string message) : base(message)
    {
    }

    public ConnectionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

public interface IConnection : ILifeCycle
{
    IManagement Management();

    IPublisherBuilder PublisherBuilder();

    IConsumerBuilder ConsumerBuilder();

    public ReadOnlyCollection<IPublisher> GetPublishers();
}
