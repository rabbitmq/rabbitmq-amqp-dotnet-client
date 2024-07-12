namespace RabbitMQ.AMQP.Client;

public class ModelException(string message) : Exception(message);

public class PreconditionFailedException(string message) : Exception(message);

public interface IManagement : IClosable
{
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);

    IQueueDeletion QueueDeletion();

    IExchangeSpecification Exchange();

    IExchangeSpecification Exchange(string name);

    IExchangeDeletion ExchangeDeletion();

    IBindingSpecification CreateBindingSpecification();

    ITopologyListener TopologyListener();
}
