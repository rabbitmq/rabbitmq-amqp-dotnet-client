using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client;

public class ConnectionException(string? message) : Exception(message);

public interface IConnection : IResourceStatus, IClosable
{
    IManagement Management();

    IPublisherBuilder PublisherBuilder();

    IConsumerBuilder ConsumerBuilder();


    public ReadOnlyCollection<IPublisher> GetPublishers();
}
