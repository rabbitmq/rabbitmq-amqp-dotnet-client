namespace RabbitMQ.AMQP.Client;

public class ModelException(string message) : Exception(message);

public class PreconditionFailedException(string message) : Exception(message);

public interface IManagement : ILifeCycle
{
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);

    Task<IQueueInfo> GetQueueInfoAsync(string queueName,
        CancellationToken cancellationToken = default);

    IQueueDeletion QueueDeletion();

    IExchangeSpecification Exchange();

    IExchangeSpecification Exchange(string name);

    IExchangeDeletion ExchangeDeletion();

    IBindingSpecification Binding();

    ITopologyListener TopologyListener();
}
